use strict;
use warnings;

use Test::More;
use Plack::Test;
use Plack::Util;
use FindBin qw/$Bin/;
use HTTP::Request::Common;

test_psgi Plack::Util::load_psgi("$Bin/../bin/divn.psgi"), sub {
    my $cb = shift;

    my $res = $cb->(GET "/");

    is $res->code, 404;
};

done_testing
