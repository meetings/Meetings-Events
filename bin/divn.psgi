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

sub todo { die "Unimplemented: " . join "\n", @_ }
sub ok   {
    my $req = shift;
    [200, ['Content-Type', 'text/javascript'], [$req->parameters->{callback} . '(' . JSON::encode_json({result => {@_}}) . ')']]
}
sub fail {
    my $req = shift;
    [200, ['Content-Type', 'text/javascript'], [$req->parameters->{callback} . '(' . JSON::encode_json({error  => {@_}}) . ')']]
}

my %sessions;

my $event_source  = 'http://dev.meetin.gs/event_source_gateway';
my $authenticator = "$event_source/authenticate";
my $fetch         = "$event_source/fetch";

our $FETCH_STARTUP_DELAY = 1;
our $FETCH_SYNC_DELAY    = 10;
our $FETCH_AMOUNT        = 0;

my $source_after  = -1;
my $source_before = -1;

my $secret = 'shared secret';

our %seen_events;

our %poll_condvars;
our %poll_timeout_condvars;

sub action (&) {
    my $action = shift;

    sub {
        my $env    = shift;
        my $req    = Plack::Request->new($env);
        my $params = eval { JSON::decode_json($req->parameters->{params} // "{}") };

        #warn "Request [" . $req->uri . "]\n";

        if ($@) {
            warn "Invalid JSON params: $@\n";
            return fail $req, code => 100, message => 'Invalid params';
        }

        $req->parameters->{callback} //= 'parseResult';

        sub {
            my $respond = shift;

            $action->($params, $req, $respond);
        }
    }
}

our $fetch_timer = AnyEvent->timer(
    after => $FETCH_STARTUP_DELAY,
    cb    => \&fetch
);

sub fetch {
    my $uri = "$fetch?secret=" . uri_escape($secret) . "&after=$source_after&before=$source_before";

    if (defined $FETCH_AMOUNT) {
        $uri .= "&amount=$FETCH_AMOUNT";
        $FETCH_AMOUNT = undef;
    }

    warn "Fetching events ($uri)...\n";

    http_get $uri,
        sub {
            my ($data, $headers) = @_;

            my $response = JSON::decode_json($data);
            my $result   = $response->{result};

            my $after  = $result->{after};
            my $before = $result->{before};
            my $events = $result->{events};

            warn "Got " . @$events . " events\n";

            for my $event (@$events) {
                my $id        = $event->{id};
                my $updated   = $event->{updated};
                my $timestamp = $event->{timestamp};
                my $topics    = Set::Object->new(@{$event->{topics}});
                my $security  = Set::Object->new(map { Set::Object->new(@$_) } @{$event->{security}});

                warn "New event $id\n";

                next if exists $seen_events{$id} and $updated <= $seen_events{$id}{updated};

                warn "Adding to subscriptions...\n";

                $seen_events{$id}{updated} = $updated;

                while (my ($session_id, $session) = each %sessions) {
                    warn "- Session $session_id\n";
                    my $new_events = 0;

                    while (my ($subscription_name, $subscription) = each %{ $session->{subscriptions} }) {
                        warn "   - Subscription '$subscription_name'\n";
                        my $limit_topics   = $subscription->{limit_topics};
                        my $exclude_topics = $subscription->{exclude_topics};
                        warn "Subscription $subscription_name\n";
                        warn "Event topics: $topics\n";
                        warn "Limit topics: $limit_topics\n";
                        if ($timestamp >= $subscription->{start}
                                and ($subscription->{finish} == -1 or $timestamp <= $subscription->{finish})
                                and grep { $_->contains(@$_) } @$limit_topics
                            and not grep { $_->contains(@$_) } @$exclude_topics
                            and grep { $session->{security}->contains(@$_) } @$security)
                        {
                            warn "     * Adding $id to $subscription_name\n";
                            push @{ $subscription->{incoming} }, $event;
                            $new_events = 1;
                        }
                    }

                    if ($new_events) {
                        warn "Calling cb for '$session_id'\n";
                        $poll_condvars{$session_id}->send('new');
                    }
                }
            }

            $source_after  = $before;
            $source_before = -1;

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

        warn "\tSubscription $name\n";

        my @events = grep { not $_->{id} ~~ $ignore } @{ $subscription->{incoming} };

        if (@events) {
            push @{ $result{$name}{new} }, @events;
            push @{ $result{$name}{confirmed} }, map { $_->{id} } @events;

            $subscription->{incoming} = [];
        }
    }

    return %result;
}

builder {
    #enable 'SimpleLogger', level => 'debug';

    mount '/open' => action {
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

                warn "New session: $session_id\n";

                $sessions{$session_id} = {
                    token    => $token,
                    security => Set::Object->new(@{ $response->{result}{security} || [] }),
                };

                $respond->(ok $request, session => $session_id);
            };
    };

    mount '/close' => action {
        my ($params, $request, $respond) = @_;

        my $session_id = $params->{session} or do {
            $respond->(fail $request,
                code    => 100,
                message => "Session required"
            );
            return
        };

        if (delete $sessions{$session_id}) {
            $respond->(ok);
        } else {
            $respond->(fail $request,
                code    => 201,
                message => "Session does not exist"
            );
        }
    };

    mount '/subscribe' => action {
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
        warn "Start: $start, finish: $finish\n";
        warn "Limit: $limit_topics\n";
        warn "Exclude: $exclude_topics\n";

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

    mount '/unsubscribe' => action {
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

    mount '/poll' => action {
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

        my $amount     = $params->{amount};
        my $received   = $params->{received};

        my $open_time = time;

        if (my $old_poll = $poll_condvars{$session_id}) {
            $old_poll->send('fail');
        }

        my $poll = $poll_condvars{$session_id} = AnyEvent->condvar;

        $poll_timeout_condvars{$session_id} = AnyEvent->timer(
            after => 25,
            cb    => sub { $poll->send('timeout') }
        );

        $poll->cb(sub {
            my @msg = shift->recv;

            warn "Poll callback for '$session_id': @msg\n";

            if ($msg[0] eq 'fail') {
                $respond->(fail $request,
                    code    => 600,
                    message => "TODO"
                );
                return
            } elsif ($msg[0] eq 'timeout') {
                $respond->(fail $request,
                    code    => 603,
                    message => "Timeout"
                );
                return
            }

            my %result = get_new_events_for_session($session_id, $amount, $received);

            $respond->(ok $request, %result);
        });
    };

    mount '/nudge' => action {
        my ($params, $request, $respond) = @_;

        $fetch_timer = AnyEvent->timer(
            after => 0.1,
            cb    => \&fetch
        );

        $respond->(ok);
    };
}
