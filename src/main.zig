const std = @import("std");
// Uncomment this block to pass the first stage
const net = std.net;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!\n", .{});

    // Uncomment this block to pass the first stage
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    // var client = try server.accept();
    //
    // try stdout.print("Connection received {} is sending data\n", .{client.address});
    //
    // const message = "+PONG\r\n";
    // _ = try client.stream.write(message);

    var client = try server.accept();
    try stdout.print("Connection received {} is sending data\n", .{client.address});

    // _ = try client.stream.read(buffer);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var buffer = try client.stream.reader().readAllAlloc(allocator, 1024);
    defer allocator.free(buffer);
    const reader = client.stream.reader();
    // var buffer: [1024]u8 = undefined;

    const bytes_have_been_read = try reader.read(&buffer);

    while (bytes_have_been_read > 0) {
        const message = "+PONG\r\n";
        _ = try client.stream.writeAll(message);

        try stdout.print("{} says {s}\n", .{ client.address, message });
    }

    client.stream.close();
}
