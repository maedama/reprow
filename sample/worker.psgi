use Plack::Request;
my $app = sub {
    my $env = shift;
    my $req = Plack::Request->new($env);
    print $req->raw_body, "\n";
    return [
        200,
        [
            "Content-Type" => "text/plain"
        ],
        ["Hello World\n"],
    ];
}
