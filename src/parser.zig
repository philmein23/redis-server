const Command = @import("type.zig").Command;
const Arg = @import("type.zig").Arg;
const Loc = @import("type.zig").Loc;
const Tag = @import("type.zig").Tag;
const RedisStore = @import("store.zig").RedisStore;
const std = @import("std");

pub const Parser = struct {
    buffer: [:0]const u8,
    curr_index: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, buffer: [:0]const u8) Parser {
        return Parser{ .buffer = buffer, .curr_index = 0, .allocator = allocator };
    }

    pub fn _parse(self: *Parser) !std.ArrayList(Command) {
        var cmds = std.ArrayList(Command).init(self.allocator);

        // in case an error occurs, we want to deallocate resources from the array list to prevent memory leak
        errdefer {
            cmds.deinit();
        }
        while (self.peek() != 0) {
            const cmd = try self.parse();

            try cmds.append(cmd);
        }

        return cmds;
    }

    pub fn parse(self: *Parser) !Command {
        var command = Command{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .args = undefined };
        // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"

        if (self.peek() == '*') {
            self.next();
        }

        while (std.ascii.isDigit(self.peek())) {
            self.next();
        }
        try self.expect_return_new_line_bytes();

        if (self.peek() == '$') {
            self.next();
        }

        while (std.ascii.isDigit(self.peek())) {
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

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "get")) |_| {
                command.tag = Tag.get;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "info")) |_| {
                command.tag = Tag.info;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "replconf")) |_| {
                command.tag = Tag.replconf;
            }

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "psync")) |_| {
                command.tag = Tag.psync;
            }
        }

        try self.expect_return_new_line_bytes();

        switch (command.tag) {
            Tag.ping => return command,
            Tag.echo, Tag.get => {
                command.args[0] = try self.parse_string();
                try self.expect_return_new_line_bytes();

                return command;
            },
            Tag.set => {
                command.args[0] = try self.parse_string();

                try self.expect_return_new_line_bytes();

                command.args[1] = try self.parse_string();

                try self.expect_return_new_line_bytes();

                if (self.peek() == '$') {
                    self.next();
                }

                if (self.peek() == '2') {
                    // parse expiration option flag (px)
                    self.next();
                } else {
                    return command;
                }

                const px = try self.parse_string();

                if (std.ascii.indexOfIgnoreCase(self.buffer[px.loc.start .. px.loc.end + 1], "px")) |_| {
                    try self.expect_return_new_line_bytes();

                    command.opt = try self.parse_string();
                }
                try self.expect_return_new_line_bytes();

                return command;
            },
            Tag.info => {
                command.args[0] = try self.parse_string();

                try self.expect_return_new_line_bytes();

                return command;
            },
            Tag.replconf => {
                var i: usize = 0;
                while (std.ascii.isASCII(self.peek()) and self.peek() != 0) {
                    const arg = try self.parse_string();

                    command.args[i] = arg;

                    i += 1;

                    try self.expect_return_new_line_bytes();
                }

                return command;
            },
            Tag.psync => {
                if (self.peek() == '$') {
                    self.next();
                }

                while (std.ascii.isDigit(self.peek())) {
                    self.next();
                }

                try self.expect_return_new_line_bytes();

                var arg = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };
                if (self.peek() == '?') {
                    self.next();

                    arg.content = "?";
                    command.args[0] = arg;
                }

                try self.expect_return_new_line_bytes();

                if (self.peek() == '$') {
                    self.next();
                }

                while (std.ascii.isDigit(self.peek())) {
                    self.next();
                }

                try self.expect_return_new_line_bytes();

                var arg_two = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };
                if (self.peek() == '-') {
                    self.next();
                    if (self.peek() == '1') {
                        self.next();
                        arg_two.content = "0";

                        command.args[1] = arg_two;
                    }
                }
                try self.expect_return_new_line_bytes();

                return command;
            },
        }
    }

    fn parse_string(self: *Parser) !Arg {
        if (self.peek() == '$') {
            self.next();
        }

        while (std.ascii.isDigit(self.peek())) {
            self.next();
        }

        try self.expect_return_new_line_bytes();

        if (self.peek() == '*') {
            self.next();
            return Arg{
                .loc = Loc{ .start = self.curr_index, .end = self.curr_index },
                .tag = undefined,
                .content = "*",
            };
        }

        if (self.peek() == '0') {
            self.next();
            return Arg{
                .loc = Loc{ .start = self.curr_index, .end = self.curr_index },
                .tag = undefined,
                .content = "*",
            };
        }

        if (std.ascii.isAlphanumeric(self.peek())) {
            self.next();
            var arg = Arg{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .content = undefined };

            arg.loc.start = self.curr_index;

            while (true) {
                if (std.ascii.isAlphanumeric(self.peek())) {
                    self.next();
                    arg.loc.end = self.curr_index;

                    if (self.peek() == '-') {
                        self.next();
                    }

                    continue;
                } else {
                    break;
                }
            }

            arg.content = self.buffer[arg.loc.start .. arg.loc.end + 1];

            return arg;
        } else {
            return error.BytesAreNotAlphanumeric;
        }
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

test "test PSYNC" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    const bytes = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();
    try std.testing.expectEqual(Tag.psync, command.tag);
    try std.testing.expectEqualSlices(u8, "?", command.args[0].content);
    try std.testing.expectEqualSlices(u8, "0", command.args[1].content);
}

test "test REPLCONF" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();
    try std.testing.expectEqual(Tag.replconf, command.tag);
    try std.testing.expectEqualSlices(u8, "listening-port", command.args[0].content);
    try std.testing.expectEqualSlices(u8, "6379", command.args[1].content);

    const bytes_two = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

    var parser_two = Parser.init(allocator, bytes_two);
    const command_two = try parser_two.parse();

    try std.testing.expectEqual(Tag.replconf, command_two.tag);
    try std.testing.expectEqualSlices(u8, "GETACK", command_two.args[0].content);
    try std.testing.expectEqualSlices(u8, "*", command_two.args[1].content);
}

test "test SET with expiry opt" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var store = RedisStore.init(allocator);
    defer store.deinit();

    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();
    try store.set(command.args[0].content, command.args[1].content, command.opt.?.content);
    try std.testing.expectEqual(Tag.set, command.tag);
    try std.testing.expectEqualSlices(u8, "100", command.opt.?.content);
}

test "test INFO command" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    const bytes = "*3\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();

    try std.testing.expectEqual(Tag.info, command.tag);
    try std.testing.expectEqualSlices(u8, "replication", command.args[0].content);
}

test "multiple commands" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\nyoyb\r\n*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var parser = Parser.init(allocator, bytes);
    const cmds = try parser._parse();
    defer {
        cmds.deinit();
    }

    try std.testing.expectEqual(3, cmds.items.len);
}

test "test SET and GET command" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var store = RedisStore.init(allocator);
    defer store.deinit();

    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();
    try store.set(command.args[0].content, command.args[1].content, null);
    try std.testing.expectEqual(Tag.set, command.tag);

    const bytes_two = "*3\r\n$3\r\nGET\r\n$5\r\napple\r\n";
    var parser_two = Parser.init(allocator, bytes_two);
    const command_two = try parser_two.parse();
    const get_value = try store.get(command_two.args[0].content);

    try std.testing.expectEqual(Tag.get, command_two.tag);
    try std.testing.expectEqualSlices(u8, "pear", get_value);
}

test "test parse PING command" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    const bytes = "*1\r\n$4\r\nping\r\n";

    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();

    try std.testing.expectEqual(Tag.ping, command.tag);
}

test "test parse ECHO command" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();

    try std.testing.expectEqual(Tag.echo, command.tag);
    const exp = "pineapple";
    try std.testing.expectEqualSlices(u8, exp, command.args[0].content);
}
