use Plack::Request;
my $app = sub {
    my $env = shift;
    my $req = Plack::Request->new($env);
    print $req->raw_body, "\n";
    my $is_success = rand() > 0.5;
    if ($is_success) {
        return [
            200,
            [
                "Content-Type" => "text/plain"
            ],
            [""],
        ]
    }
    else {
        return [
            500,
            [
                "Content-Type" => "text/plain"
            ],
            [""],
        ]
    }
}
