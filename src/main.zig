const std = @import("std");
// Uncomment this block to pass the first stage
const net = std.net;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!", .{});

    // Uncomment this block to pass the first stage
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    while (true) {
        const client = try server.accept();

        try stdout.print("Connection received {} is sending data", .{client.address});
        client.stream.close();

        const message = try client.stream.reader().readAllAlloc(allocator, 1024);
        defer allocator.free(message);
        try stdout.print("{} says {s}\n", .{ client.address, message });
    }

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gpa.deinit();
    // const allocator = gpa.allocator();
    //
    // const loopback = try net.Ip4Address.parse("127.0.0.1", 6379);
    // const localhost = net.Address{ .in = loopback };
    // var server = try localhost.listen(.{
    //     .reuse_port = true,
    // });
    // defer server.deinit();
    //
    // const addr = server.listen_address;
    // try stdout.print("Listening on {}, access this port to end the program\n", .{addr.getPort()});
    //
    // var client = try server.accept();
    // defer client.stream.close();
    //
    // try stdout.print("Connection received! {} is sending data.\n", .{client.address});
    //
    // const message = try client.stream.reader().readAllAlloc(allocator, 1024);
    // defer allocator.free(message);
    //
    // try stdout.print("{} says {s}\n", .{ client.address, message });
}
