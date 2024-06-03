const std = @import("std");
const net = std.net;

fn write(client_connection: net.Server.Connection, stdout: anytype) !void {
    defer client_connection.stream.close();

    var buffer: [1024]u8 = undefined;

    const reader = client_connection.stream.reader();

    try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

    // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"
    const bytes_read = try reader.read(&buffer);

    if (bytes_read == 0) return;

    // while (bytes_read > 0) {

    // std.mem.lastIndexOfScalar([1024]u8, &buffer, "echo");

    const echo = "echo";
    var byte_offset: usize = 0;
    if (std.ascii.indexOfIgnoreCase(&buffer, echo)) |fi| {
        std.debug.print("Found index: {?}\n", .{fi});
        std.debug.print("Byte encoding: {?}\n", .{buffer[fi]});

        byte_offset = fi + echo.len - 1;

        byte_offset += 7; // skip non-alphabetic bytes
        std.debug.print("Starting character: {?}\n", .{buffer[byte_offset]});

        var end_index = byte_offset;
        var string: []u8 = "";
        while (true) {
            if (std.ascii.isAlphabetic(buffer[end_index])) {
                std.debug.print("Char to be echoed: {?}\n", .{buffer[end_index]});
                end_index += 1;

                continue;
            }
            string = buffer[byte_offset..end_index];

            std.debug.print("String to be echoed: {s}\n", .{string});
            break;
        }

        const terminator = "\r\n";
        // const bulk_string_type = "$";
        const length = string.len;

        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = gpa.deinit();

        const allocator = gpa.allocator();

        const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, string, terminator });
        std.debug.print("String to be echoed 2: {s}\n", .{resp});
        defer allocator.free(resp);

        _ = try client_connection.stream.writeAll(resp);
    }

    // bytes_read = try reader.read(&buffer);
    // }
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

            try threads.append(try std.Thread.spawn(.{}, write, .{ client_connection, stdout }));
        }

        for (threads.items) |thread| thread.detach();
    }
}
