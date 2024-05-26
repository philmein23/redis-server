const std = @import("std");
// Uncomment this block to pass the first stage
const net = std.net;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gpa.deinit();
    // const allocator = gpa.allocator();
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!", .{});

    // Uncomment this block to pass the first stage
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var count: u32 = 1;
    while (true) {
        var client = try server.accept();

        try stdout.print("Connection received {} is sending data\n", .{client.address});

        const message = "+PONG\r\n";
        _ = try client.stream.write(message);
        client.stream.close();

        count += 1;

        if (count == 2) {
            try stdout.print("About to close....{}\n", .{client.address});
        }

        try stdout.print("{} says {s}\n", .{ client.address, message });
    }
}
