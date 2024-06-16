const std = @import("std");
const net = std.net;

const Loc = struct { start: usize, end: usize };
const Tag = enum { echo, ping, set };
const Command = struct { loc: Loc, tag: Tag, arg: Arg };
const Arg = struct { loc: Loc, tag: Tag, content: []const u8 };

const Parser = struct {
    set_map: std.StringHashMap([]const u8),
    buffer: []const u8,
    curr_index: usize,

    pub fn init(buffer: []const u8, alloc: std.mem.Allocator) Parser {
        return Parser{ .set_map = std.StringHashMap([]const u8).init(alloc), .buffer = buffer, .curr_index = 0 };
    }

    pub fn deinit(self: *Parser) void {
        self.set_map.deinit();
    }
    pub fn parse(self: *Parser) !Command {
        const arg = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };
        var command = Command{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .arg = arg };
        // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"
        if (self.peek() == '*') {
            self.next();
        }

        if (std.ascii.isDigit(self.peek())) {
            self.next();
        } else {
            return error.ParsingErrorUnexpectedCharacter;
        }

        try self.expect_return_new_line_bytes();

        if (self.peek() == '$') {
            self.next();
        }

        if (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        try self.expect_return_new_line_bytes();

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

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "echo")) |_| {
                command.tag = Tag.echo;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "ping")) |_| {
                command.tag = Tag.ping;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "set")) |_| {
                command.tag = Tag.set;
            }
        }

        try self.expect_return_new_line_bytes();

        if (command.tag == Tag.ping) {
            return command;
        }

        if (self.peek() == '$') {
            self.next();
        }

        if (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        try self.expect_return_new_line_bytes();

        if (std.ascii.isAlphanumeric(self.peek())) {
            self.next();
            command.arg.loc.start = self.curr_index;

            while (true) {
                if (std.ascii.isAlphanumeric(self.peek())) {
                    self.next();
                    command.arg.loc.end = self.curr_index;

                    continue;
                } else {
                    break;
                }
            }

            std.debug.print("Command ARG content {s}\n", .{self.buffer[command.arg.loc.start .. command.arg.loc.end + 1]});

            command.arg.content = self.buffer[command.arg.loc.start .. command.arg.loc.end + 1];
        }

        if (command.tag == Tag.ping) {
            return command;
        }
        if (command.tag == Tag.set) {
            try self.expect_return_new_line_bytes();

            if (self.peek() == '$') {
                self.next();
            }

            if (std.ascii.isDigit(self.peek())) {
                self.next();
            }

            try self.expect_return_new_line_bytes();

            if (std.ascii.isAlphanumeric(self.peek())) {
                self.next();
                var new_value = Arg{ .loc = undefined, .tag = .set, .content = undefined };
                new_value.loc.start = self.curr_index;

                while (true) {
                    if (std.ascii.isAlphanumeric(self.peek())) {
                        self.next();
                        new_value.loc.end = self.curr_index;

                        continue;
                    } else {
                        break;
                    }
                }

                new_value.content = self.buffer[new_value.loc.start .. new_value.loc.end + 1];

                std.debug.print("SET mapping key: {s}, value: {s}\n", .{ command.arg.content, new_value.content });
                try self.set_map.put(command.arg.content, new_value.content);
            }
        }

        try self.expect_return_new_line_bytes();

        return command;
    }
    fn next(self: *Parser) void {
        self.curr_index += 1;
    }
    fn peek(self: *Parser) u8 {
        return self.buffer[self.curr_index + 1];
    }

    fn expect_return_new_line_bytes(self: *Parser) !void {
        if (self.peek() == '\r') {
            self.next();
        } else {
            return error.ExpectedCarriageReturnByte;
        }

        if (self.peek() == '\n') {
            self.next();
        } else {
            return error.ExpectedNewLineByte;
        }
    }
};

test "test SET command" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const alloc = gpa.allocator();
    var parser = Parser.init(bytes, alloc);
    const command = try parser.parse();

    std.debug.print("Command ARG content: {s}\n", .{command.arg.content});
    try std.testing.expectEqual(Tag.set, command.tag);

    parser.deinit();
}

test "test GET command" {}

test "test parse PING command" {
    const bytes = "*1\r\n$4\r\nping\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const alloc = gpa.allocator();
    var parser = Parser.init(bytes, alloc);
    const command = try parser.parse();

    std.debug.print("Command ARG content: {s}\n", .{command.arg.content});
    try std.testing.expectEqual(Tag.ping, command.tag);

    parser.deinit();
}

test "test parse ECHO command" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const alloc = gpa.allocator();
    var parser = Parser.init(bytes, alloc);
    const command = try parser.parse();

    std.debug.print("Command ARG content: {s}\n", .{command.arg.content});
    try std.testing.expectEqual(Tag.echo, command.tag);
    const exp = "pineapple";
    std.debug.print("Command ARG expected ptr: {*}, actual ptr: {*}\n", .{ exp.ptr, command.arg.content.ptr });
    std.debug.print("Command ARG expected slice: {s}, actual slice: {s}\n", .{ exp, command.arg.content });
    try std.testing.expectEqualSlices(u8, exp, command.arg.content);

    parser.deinit();
}

fn handle_echo(client_connection: net.Server.Connection, arg: Arg) !void {
    const terminator = "\r\n";
    const length = arg.content.len;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const resp = try std.fmt.allocPrint(allocator, "${d}{s}{s}{s}", .{ length, terminator, arg.content, terminator });
    defer allocator.free(resp);

    _ = try client_connection.stream.writeAll(resp);
}

fn handle_ping(client_connection: net.Server.Connection) !void {
    std.debug.print("Command PING\n", .{});
    try client_connection.stream.writeAll("+PONG\r\n");
}

fn handle_connection(client_connection: net.Server.Connection, stdout: anytype) !void {
    defer client_connection.stream.close();

    var buffer: [1024]u8 = undefined;

    const reader = client_connection.stream.reader();

    try stdout.print("Connection received {} is sending data\n", .{client_connection.address});

    while (try reader.read(&buffer) > 0) {
        var parser = Parser{ .buffer = &buffer, .curr_index = 0 };

        const command = try parser.parse();

        switch (command.tag) {
            Tag.echo => try handle_echo(client_connection, command.arg),
            Tag.ping => try handle_ping(client_connection),
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

            try threads.append(try std.Thread.spawn(.{}, handle_connection, .{ client_connection, stdout }));
        }

        for (threads.items) |thread| thread.detach();
    }
}
