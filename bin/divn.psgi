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
use URI::Escape qw/uri_escape/;
use Set::Object;
use Data::Dump qw/dump/;

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

my $peer          = 'http://localhost:8082';
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

sub logger {
    my ($level, $message) = @_;

    warn "[$level] $message\n";
}

# Create an action easily

sub action (&) {
    my $action = shift;

    sub {
        my $env    = shift;
        my $req    = Plack::Request->new($env);
        my $params = eval { JSON::decode_json($req->parameters->{params} // "{}") };

        $req->parameters->{callback} //= 'parseResult';

        if ($@) {
            logger error => "Invalid JSON params: $@";
            return fail $req,
                code    => 100,
                message => 'Invalid params';
        }

        sub {
            my $respond = shift;

            $action->($params, $req, $respond);
        }
    }
}

sub action_with_session (&) {
    my $action = shift;

    sub {
        my $env    = shift;
        my $req    = Plack::Request->new($env);
        my $params = eval { JSON::decode_json($req->parameters->{params} // "{}") };

        $req->parameters->{callback} //= 'parseResult';

        if ($@) {
            warn "Invalid JSON params: $@\n";
            return fail $req,
                code    => 100,
                message => 'Invalid params';
        }

        my $session_id = $params->{session};

        unless ($session_id) {
            return fail $req,
                code => 201,
                message => "Session ID required"
        }

        my $session = $sessions{$session_id};

        unless ($session) {
            if ($params->{forwarded} or not $peer) {
                return fail $req,
                    code => 201,
                    message => "Session does not exist"

            } else {
                return sub {
                    my $respond = shift;

                    propagate_request($params, $req, $respond);
                };
            }
        }

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

    #warn "Fetching events ($uri)...\n";

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

            #warn "Got " . @$events . " events\n";

            my %new_events;

            for my $event (@$events) {
                my $id        = $event->{id};
                my $updated   = $event->{updated};
                my $timestamp = $event->{timestamp};
                my $topics    = Set::Object->new(@{$event->{topics}});
                my $security  = Set::Object->new(map { Set::Object->new(@$_) } @{$event->{security}});

                next if exists $seen_events{$id} and $updated <= $seen_events{$id}{updated};

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

                        if (grep /wiki_page/, $topics->elements) {
                            logger info => join("\n",
                                "*** $subscription_name ***",
                                "Topics: $topics",
                                "Limit:  $subscription->{limit_topics}",
                                "Security: $security",
                                "Timestamp: $timestamp");
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

    logger debug => "Polling $session_id...";

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

    my $original_path = $request->script_name;

    $params->{forwarded} = 1;

    my $encoded_params = JSON::encode_json($params);

    my $url = URI->new(qq[$peer$original_path?params=$encoded_params]);

    warn "Propagating request to $url\n";

    http_get $url, sub {
        my ($data, $headers) = @_;

        warn "Propagation response: $data\n";

        my $psgi_headers = to_psgi_headers($headers);

        my $response = [ 200, $psgi_headers, [ $data ] ];

        warn "Response to client: " . dump($response) . "\n";

        $respond->($response);
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

my $close = action_with_session {
    my ($params, $request, $respond) = @_;

    my $session_id = $params->{session};

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

my $subscribe = action_with_session {
    my ($params, $request, $respond) = @_;

    my $session_id     = $params->{session};
    my $name           = $params->{name};
    my $limit_topics   = Set::Object->new(map { Set::Object->new(@$_) } @{ $params->{limit_topics}   // [[]] });
    my $exclude_topics = Set::Object->new(map { Set::Object->new(@$_) } @{ $params->{exclude_topics} // [] });

    my $start  = $params->{start}  // time;
    my $finish = $params->{finish} // -1;

    if (exists $sessions{$session_id}{subscriptions}{$name}) {
        $respond->(fail $request,
            code    => 302,
            message => "Subscription already exists"
        );
        return
    }

    logger info => "New subscription $name";

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

my $unsubscribe = action_with_session {
    my ($params, $request, $respond) = @_;

    my $session_id = $params->{session};
    my $name       = $params->{subscription};

    if (delete $sessions{$session_id}{subscriptions}{$name}) {
        $respond->(ok);
    } else {
        $respond->(fail $request,
            code    => 502,
            message => "Subscription does not exist"
        );
    }
};

my $poll = action_with_session {
    my ($params, $request, $respond) = @_;

    my $session_id = $params->{session};

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
