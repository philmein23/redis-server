const std = @import("std");
// Uncomment this block to pass the first stage
const net = std.net;

fn write() !void {
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var client_connection = try server.accept();
    var buffer: [1024]u8 = undefined;

    const reader = client_connection.stream.reader();
    const bytes_have_been_read = try reader.read(&buffer);

    const stdout = std.io.getStdOut().writer();
    try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

    while (bytes_have_been_read > 0) {
        const message = "+PONG\r\n";
        _ = try client_connection.stream.writeAll(message);
    }

    client_connection.stream.close();
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var threads = std.ArrayList(std.Thread).init(allocator);
    defer threads.deinit();

    const cpus = try std.Thread.getCpuCount();

    for (0..cpus) |_| {
        try threads.append(try std.Thread.spawn(.{}, write, .{}));
    }

    for (threads.items) |thread| thread.join();
}
