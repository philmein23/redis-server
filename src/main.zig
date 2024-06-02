const std = @import("std");
const net = std.net;

fn write(client_connection: net.Server.Connection) !void {
    defer client_connection.stream.close();

    var buffer: [1024]u8 = undefined;

    const reader = client_connection.stream.reader();

    const stdout = std.io.getStdOut().writer();
    try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

    while (try reader.read(&buffer) > 0) |bytes_read| {
        const message = "+PONG\r\n";
        // std.mem.lastIndexOfScalar(comptime T: type, slice: []const T, value: T);

        try stdout.print("Bytes read here : {}\n", .{bytes_read});
        _ = try client_connection.stream.writeAll(message);
    }
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!\n", .{});
    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var threads = std.ArrayList(std.Thread).init(allocator);
    defer threads.deinit();

    const cpus = try std.Thread.getCpuCount();
    try stdout.print("CPU core count {}\n", .{cpus});

    while (true) {
        for (0..cpus) |_| {
            const client_connection = try server.accept();

            try threads.append(try std.Thread.spawn(.{}, write, .{client_connection}));
        }

        for (threads.items) |thread| thread.detach();
    }
}
