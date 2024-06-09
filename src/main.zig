const std = @import("std");
const net = std.net;

const Loc = struct { start: usize, end: usize };
const Tag = enum { echo, ping, arg };
const Command = struct { loc: Loc, tag: Tag };
const Arg = struct { loc: Loc, tag: Tag, content: []u8 };

var Parser = struct {
    buffer: []u8,
    curr_index: usize,
    pub fn parse(self: *Parser) !.{ Command, Arg } {
        var command = Command{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined };
        const arg = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined };
        // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"
        if (self.peek() == '*') {
            self.next();
        }

        if (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        if (self.peek() == '\r') {
            self.next();
        }

        if (self.peek() == '\n') {
            self.next();
        }

        // parse command
        if (std.ascii.isAlphabetic(self.peek())) {
            self.next();
            command.loc.start = self.curr_index;

            while (true) {
                if (std.ascii.isAlphabetic(self.peek())) {
                    self.next();
                    command.loc.end = self.curr_index;

                    continue;
                } else {
                    break;
                }
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start..command.loc.end], "echo")) |_| {
                command.tag = Tag.echo;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start..command.loc.end], "ping")) |_| {
                command.tag = Tag.ping;
            }
        }

        if (self.peek() == '\r') {
            self.next();
        }

        if (self.peek() == '\n') {
            self.next();
        }

        if (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        if (self.peek() == '\r') {
            self.next();
        }

        if (self.peek() == '\n') {
            self.next();
        }

        return .{ command, arg };
    }
    fn next(self: *Parser) void {
        self.curr_index += 1;
    }
    fn peek(self: *Parser) u8 {
        return self.buffer[self.curr_index + 1];
    }
};

fn write(client_connection: net.Server.Connection, stdout: anytype) !void {
    defer client_connection.stream.close();

    var buffer: [1024]u8 = undefined;

    const reader = client_connection.stream.reader();

    try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

    while (try reader.read(&buffer) > 0) {
        var command: []u8 = "";
        var byte_offset: usize = 8; // skip control sequence of bytes that dont need to be parsed
        var command_index_end = byte_offset;
        while (true) {
            if (std.ascii.isAlphabetic(buffer[command_index_end])) {
                std.debug.print("Command char: {?}\n", .{buffer[command_index_end]});
                command_index_end += 1;

                continue;
            }
            command = buffer[byte_offset..command_index_end];

            std.debug.print("Sliced command: {s}\n", .{command});

            break;
        }

        if (std.ascii.indexOfIgnoreCase(command, "ping")) |_| {
            try client_connection.stream.writeAll("+PONG\r\n");
        } else {
            // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"
            if (std.ascii.indexOfIgnoreCase(&buffer, command)) |fi| {
                std.debug.print("Found starting index: {?}. Byte encoding: {?}\n", .{ fi, buffer[fi] });

                byte_offset = fi + command.len - 1;

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

                    break;
                }

                const terminator = "\r\n";
                const length = string.len;

                var gpa = std.heap.GeneralPurposeAllocator(.{}){};
                defer _ = gpa.deinit();

                const allocator = gpa.allocator();

                const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, string, terminator });
                defer allocator.free(resp);

                _ = try client_connection.stream.writeAll(resp);
            }
        }
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

            try threads.append(try std.Thread.spawn(.{}, write, .{ client_connection, stdout }));
        }

        for (threads.items) |thread| thread.detach();
    }
}
