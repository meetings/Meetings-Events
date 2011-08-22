#!/usr/bin/env perl

# PODNAME: divn
# ABSTRACT: Dicole event server

use 5.010;

use strict;
use warnings;

use AnyEvent::Socket;
use AnyEvent::Handle;
use AnyEvent::HTTP;
use JSON ();
use UUID::Tiny ':std';
use Plack::Request;
use Plack::Builder;
use Data::Dumper 'Dumper';
use URI::Escape qw/uri_escape/;
use Set::Object;

# Utility objects for easy responses

sub ok {
    my $req = shift;
    _build_response($req, { result => { @_ } });
}

sub fail {
    my $req = shift;
    _build_response($req, { error => { @_ } });
}

sub _build_response {
    my ($req, $response) = @_;
    [ 200,
      [ 'Content-Type', 'text/javascript' ],
      [ $req->parameters->{callback} . '(' .
          JSON::encode_json($response) .
        ')'
      ]
    ]
}

# Globals etc.

my %sessions;

my $peer          = 'http://localhost:8081';
my $gateway       = 'http://servers.dicole.com:20026/nudge';
my $event_source  = 'http://meetin.gs/event_source_gateway';
my $authenticator = "$event_source/authenticate";
my $fetch         = "$event_source/fetch";

our $FETCH_STARTUP_DELAY = 1;
our $FETCH_SYNC_DELAY    = 10;
our $FETCH_AMOUNT        = 0;

our $REAP_TIMEOUT = 300;

my $source_after  = -1;
my $source_before = -1;

my $secret = 'shared secret';

our %seen_events;

our $fetch_timer = AnyEvent->timer(
    after => $FETCH_STARTUP_DELAY,
    cb    => \&fetch
);

# Create an action easily

sub action (&) {
    my $action = shift;

    sub {
        my $env    = shift;
        my $req    = Plack::Request->new($env);
        my $params = eval { JSON::decode_json($req->parameters->{params} // "{}") };

        if ($@) {
            warn "Invalid JSON params: $@\n";
            return fail $req,
                code    => 100,
                message => 'Invalid params';
        }

        $req->parameters->{callback} //= 'parseResult';

        sub {
            my $respond = shift;

            $action->($params, $req, $respond);
        }
    }
}

# Subroutines

sub fetch {
    my $uri = "$fetch?secret=" . uri_escape($secret) . "&after=$source_after&before=$source_before&gateway=" . uri_escape($gateway);

    if (defined $FETCH_AMOUNT) {
        $uri .= "&amount=$FETCH_AMOUNT";
        $FETCH_AMOUNT = undef;
    }

    warn "Fetching events ($uri)...\n";

    http_get $uri,
        sub {
            my ($data, $headers) = @_;

            my $response = eval { JSON::decode_json($data) };

            if ($@) {
                warn "Invalid response from source: $@\n";
                goto DONE;
            }

            my $result = $response->{result};

            my $after  = $result->{after};
            my $before = $result->{before};
            my $events = $result->{events};

            warn "Got " . @$events . " events\n";

            my %new_events;

            for my $event (@$events) {
                my $id        = $event->{id};
                my $updated   = $event->{updated};
                my $timestamp = $event->{timestamp};
                my $topics    = Set::Object->new(@{$event->{topics}});
                my $security  = Set::Object->new(map { Set::Object->new(@$_) } @{$event->{security}});

                #warn "New event $id\n";
                #warn "Topics: $topics\n";
                #warn "Security: $security\n";
                #warn "Timestamp: $timestamp\n";

                next if exists $seen_events{$id} and $updated <= $seen_events{$id}{updated};

                #warn "Adding to subscriptions...\n";

                $seen_events{$id}{updated} = $updated;

                while (my ($session_id, $session) = each %sessions) {
                    while (my ($subscription_name, $subscription) = each %{ $session->{subscriptions} }) {
                        my $after_start = $timestamp >= $subscription->{start}
                            ;#or warn "Event is before interest\n";
                        my $before_finish = ($subscription->{finish} == -1 or $timestamp <= $subscription->{finish})
                            ;#or warn "Event is after interest\n";
                        my $is_interested   =     grep { $topics->contains(@$_) } @{ $subscription->{limit_topics}   }
                            ;#or warn "Not interested\n";
                        my $is_not_excluded = not grep { $topics->contains(@$_) } @{ $subscription->{exclude_topics} }
                            ;#or warn "Excluded\n";

                        my $has_permissions =     eval { grep { $session->{security}->contains(@$_) } @$security }
                            ;#or warn "Not permitted\n";

                        if ($@) {
                            warn "WTF: $@\n";
                        }

                        if ($after_start and $before_finish and $is_interested and $is_not_excluded and $has_permissions) {
                            push @{ $subscription->{incoming} }, { map { $_ => $event->{$_} } qw/id updated topics data timestamp version/ };
                            $new_events{$session_id} = 1;
                        }
                    }
                }
            }

            for my $session_id (keys %new_events) {
                $sessions{$session_id}{poll_cv}->send('new') if $sessions{$session_id}{poll_cv};
            }

            $source_after  = $before;
            $source_before = -1;

            DONE:

            if ($source_after != -1) {
                $fetch_timer = AnyEvent->timer(
                    after    => $FETCH_SYNC_DELAY,
                    cb       => \&fetch
                );
            }

        };
}

sub get_new_events_for_session {
    my ($session_id, $amount, $received) = @_;

    my $session = $sessions{$session_id};

    warn "Polling $session_id...\n";

    my %result;

    while (my ($name, $subscription) = each %{ $session->{subscriptions} }) {
        my $ignore = $received->{$name} // [];

        #warn "- Subscription $name\n";

        my @events = grep { not $_->{id} ~~ $ignore } @{ $subscription->{incoming} };

        if (@events) {
            push @{ $result{$name}{new} }, @events;
            push @{ $result{$name}{confirmed} }, map { $_->{id} } @events;

            $subscription->{incoming} = [];
        }
    }

    return %result;
}

sub propagate_request {
    my ($params, $request, $respond) = @_;

    my $original_path = $request->request_uri;

    http_get "$peer/$original_path", sub {
        my ($data, $headers) = @_;

        my $psgi_headers = to_psgi_headers($headers);

        $respond->([ 200, $psgi_headers, $data ]);
    };
}

sub to_psgi_headers {
    my $hash = shift;

    my @result;

    while (my ($key, $value) = each %$hash) {
        my @values = split /,/, $value;

        push @result, [ $key, $_ ] for @values;
    }

    return \@result
}

# Actions

my $open = action {
    my ($params, $request, $respond) = @_;

    my $token = $params->{token} or do {
        $respond->(fail $request,
            code    => 100,
            message => "Token required"
        );
        return
    };

    my $domain = $params->{domain};

    http_get "$authenticator?token=$token" . ($domain ? "&domain=$domain" : ()),
        sub {
            my ($data, $headers) = @_;

            my $response = eval { JSON::decode_json($data) };

            if ($@) {
                $respond->(fail $request,
                    code    => 101,
                    message => "Communication error"
                );
            }

            my $session_id = create_uuid_as_string(UUID_RANDOM);

            #warn "New session: $session_id\n";

            $sessions{$session_id} = {
                token    => $token,
                security => Set::Object->new(@{ $response->{result}{security} || [] }),
                reaper   => AnyEvent->timer(
                    after => $REAP_TIMEOUT,
                    cb    => sub {
                        delete $sessions{$session_id};
                    }
                )
            };

            $respond->(ok $request, session => $session_id);
        };
};

my $close = action {
    my ($params, $request, $respond) = @_;

    my $session_id = $params->{session}
        or goto &propagate_request;

    if (my $session = delete $sessions{$session_id}) {
        $session->{poll_cv}->send('close') if $session->{poll_cv};
        $respond->(ok);
    } else {
        $respond->(fail $request,
            code    => 201,
            message => "Session does not exist"
        );
    }
};

my $subscribe = action {
    my ($params, $request, $respond) = @_;

    my $session_id     = $params->{session};
    my $name           = $params->{name};
    my $limit_topics   = Set::Object->new(map { Set::Object->new(@$_) } @{ $params->{limit_topics}   // [[]] });
    my $exclude_topics = Set::Object->new(map { Set::Object->new(@$_) } @{ $params->{exclude_topics} // [] });

    my $start  = $params->{start}  // time;
    my $finish = $params->{finish} // -1;

    unless (defined $session_id and exists $sessions{$session_id}) {
        $respond->(fail $request,
            code    => 301,
            message => "Invalid session"
        );
        return
    }

    if (exists $sessions{$session_id}{subscriptions}{$name}) {
        $respond->(fail $request,
            code    => 302,
            message => "Subscription already exists"
        );
        return
    }

    warn "New subscription '$name' for session '$session_id'\n";
    #warn "Start: $start, finish: $finish\n";
    #warn "Limit: $limit_topics\n";
    #warn "Exclude: $exclude_topics\n";

    $sessions{$session_id}{subscriptions}{$name} = {
        start          => $start,
        finish         => $finish,
        limit_topics   => $limit_topics,
        exclude_topics => $exclude_topics
    };

    $respond->(ok $request,
        start  => $start,
        finish => $finish
    );
};

my $unsubscribe = action {
    my ($params, $request, $respond) = @_;

    my $session_id = $params->{session};
    my $name       = $params->{subscription};

    unless (exists $sessions{$session_id}) {
        $respond->(fail $request,
            code    => 501,
            message => "Session does not exist"
        );
        return
    }

    if (delete $sessions{$session_id}{subscriptions}{$name}) {
        $respond->(ok);
    } else {
        $respond->(fail $request,
            code    => 502,
            message => "Subscription does not exist"
        );
    }
};

my $poll = action {
    my ($params, $request, $respond) = @_;

    my $session_id = $params->{session} or do {
        $respond->(fail $request,
            code    => 100,
            message => "Session required"
        );
        return
    };

    unless (exists $sessions{$session_id}) {
        #warn "Polled invalid session '$session_id'\n";
        $respond->(fail $request,
            code    => 601,
            message => "Session does not exist"
        );
        return
    }

    my $amount   = $params->{amount};
    my $received = $params->{received};

    if (my $old_poll = $sessions{$session_id}{poll_cv}) {
        $old_poll->send('fail');
    }

    my $poll = $sessions{$session_id}{poll_cv} = AnyEvent->condvar;

    $sessions{$session_id}{poll_timeout} = AnyEvent->timer(
        after => 25,
        cb    => sub { $poll->send('timeout') }
    );

    $sessions{$session_id}{reaper} = AnyEvent->timer(
        after => $REAP_TIMEOUT,
        cb    => sub {
            delete $sessions{$session_id};
        }
    );

    $poll->cb(sub {
        my ($msg) = shift->recv;

        #warn "Poll callback for '$session_id': $msg\n";

        delete $sessions{$session_id}{poll_cv};
        delete $sessions{$session_id}{poll_timeout};

        given ($msg) {
            when ('fail') {
                $respond->(fail $request,
                    code    => 604,
                    message => "Newer poll requested"
                );
            }
            when ('timeout') {
                $respond->(fail $request,
                    code    => 603,
                    message => "Timeout"
                );
            }
            when ('close') {
                $respond->(fail $request,
                    code    => 605,
                    message => "Session closed"
                );
            }
            when ('new') {
                my %result = get_new_events_for_session($session_id, $amount, $received);

                $respond->(ok $request, %result);
            }
            default {
                $respond->(fail code => 600, message => 'Internal error');
            }
        }
    });
};

my $nudge = action {
    my ($params, $request, $respond) = @_;

    warn "Nudge\n";

    $fetch_timer = AnyEvent->timer(
        after => 0,
        cb    => \&fetch
    );

    $respond->(ok);
};

builder {
    #enable 'SimpleLogger', level => 'debug';

    mount '/open'        => $open;
    mount '/close'       => $close;
    mount '/subscribe'   => $subscribe;
    mount '/unsubscribe' => $unsubscribe;
    mount '/poll'        => $poll;
    mount '/nudge'       => $nudge;
}
