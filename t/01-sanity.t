#!/usr/bin/env perl

use 5.010;
use strict;
use warnings;

use Test::More;
use Plack::Test;
use Plack::Util;
use FindBin qw/$Bin/;
use HTTP::Request::Common;

test_psgi Plack::Util::load_psgi("$Bin/../bin/divn.psgi"), sub {
    my $cb = shift;

    is $cb->(GET "/")->code,          404;
    is $cb->(GET "/open")->code,      200;
    is $cb->(GET "/close")->code,     200;
    is $cb->(GET "/subscribe")->code, 200;
    is $cb->(GET "/poll")->code,      200;
    is $cb->(GET "/nudge")->code,     200;
};

done_testing
