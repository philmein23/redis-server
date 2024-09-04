const Command = @import("type.zig").Command;
const Arg = @import("type.zig").Arg;
const Loc = @import("type.zig").Loc;
const Tag = @import("type.zig").Tag;
const RedisStore = @import("store.zig").RedisStore;
const std = @import("std");

const Command_ = union(enum) {
    ping,
    info: Info,
    set: Set,
    get: Get,
    psync,
    replconf: Replconf,
    wait: Wait,
    echo: Echo,

    const Echo = []const u8;

    const Wait = struct { num_replicas_to_ack: usize, exp: i16 };

    const Info = enum { replication };

    const Set = struct {
        key: []const u8,
        val: []const u8,
        exp: i16,
    };
    const Get = struct {
        key: []const u8,
    };

    const Replconf = union(enum) {
        ack: usize,
        getack: u8,
    };
};

pub const Token = struct {
    loc: Loc2,
    tag: Tag2,
    pub const Loc2 = struct {
        start: usize,
        end: usize,
    };
    pub const Tag2 = enum { dollar, asterisk, colon, number_literal, string_literal, question_mark, minus, carriage_return, eoc };
};

pub const Tokenizer = struct {
    buffer: [:0]const u8,
    index: usize = 0,

    const State = enum { start, int, string_literal, minus, carriage };

    pub fn init(source: [:0]const u8) Tokenizer {
        return .{ .buffer = source };
    }

    pub fn next(self: *Tokenizer) Token {
        var state: State = .start;
        var result: Token = .{
            .tag = undefined,
            .loc = .{
                .start = self.index,
                .end = undefined,
            },
        };

        while (true) : (self.index += 1) {
            const c = self.buffer[self.index];

            switch (state) {
                .start => {
                    switch (c) {
                        '\r' => {
                            state = .carriage;
                        },
                        'a'...'z', 'A'...'Z', '_' => {
                            state = .string_literal;
                            result.tag = .string_literal;
                        },
                        '0'...'9' => {
                            state = .int;
                            result.tag = .number_literal;
                        },
                        '-' => {
                            state = .minus;
                            result.tag = .minus;
                        },
                        '?' => {
                            result.tag = .question_mark;
                            self.index += 1;
                            break;
                        },
                        '*' => {
                            result.tag = .asterisk;
                            self.index += 1;
                            break;
                        },
                        '$' => {
                            result.tag = .dollar;
                            self.index += 1;
                            break;
                        },
                        ':' => {
                            result.tag = .colon;
                            self.index += 1;
                            break;
                        },
                        else => {
                            result.tag = .eoc;
                            break;
                        },
                    }
                },
                .string_literal => switch (c) {
                    'a'...'z', 'A'...'Z', '_', '-' => {
                        continue;
                    },
                    else => {
                        break;
                    },
                },
                .minus => switch (c) {
                    '1' => {
                        result.tag = .number_literal;
                        self.index += 1;
                        break;
                    },
                    else => {
                        break;
                    },
                },
                .int => switch (c) {
                    '0'...'9' => {
                        continue;
                    },
                    else => {
                        break;
                    },
                },
                .carriage => switch (c) {
                    '\n' => {
                        result.tag = .carriage_return;
                        self.index += 1;
                        break;
                    },
                    else => {
                        break;
                    },
                },
            }
        }

        result.loc.end = self.index;
        return result;
    }
};

test "echo tokenizer" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    try testTokenize(bytes, &.{
        .asterisk,
        .number_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .eoc,
    });
}

test "set tokenizer" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    try testTokenize(bytes, &.{
        .asterisk,
        .number_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .eoc,
    });
}

test "psync tokenizer" {
    const bytes = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    try testTokenize(bytes, &.{
        .asterisk,
        .number_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .string_literal,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .question_mark,
        .carriage_return,
        .dollar,
        .number_literal,
        .carriage_return,
        .number_literal,
        .carriage_return,
        .eoc,
    });
}

fn testTokenize(source: [:0]const u8, expected_token_tags: []const Token.Tag2) !void {
    var tokenizer = Tokenizer.init(source);
    for (expected_token_tags) |expected_token_tag| {
        const token = tokenizer.next();
        try std.testing.expectEqual(expected_token_tag, token.tag);
    }

    const last_token = tokenizer.next();
    try std.testing.expectEqual(source.len, last_token.loc.start);
    try std.testing.expectEqual(source.len, last_token.loc.end);
}

pub const Parser_ = struct {
    gpa: std.mem.Allocator,
    tokens: TokenList.Slice,
    source: [:0]const u8,
    tok_i: ByteOffset = 0,
    token_tags: []const Token.Tag2,
    token_starts: []const ByteOffset,

    pub const TokenList = std.MultiArrayList(struct {
        tag: Token.Tag2,
        start: ByteOffset,
    });
    pub const ByteOffset = u8;

    pub fn next_token(self: *Parser_) ByteOffset {
        const result = self.tok_i;
        self.tok_i += 1;

        return result;
    }

    pub fn eat_token(self: *Parser_, tag: Token.Tag2) ?Token.Tag2 {
        if (self.token_tags[self.tok_i] == tag) self.next_token() else null;
    }

    pub fn init(gpa: std.mem.Allocator, source: [:0]const u8) !Parser_ {
        var tokens = TokenList{};

        var tokenizer = Tokenizer.init(source);
        while (true) {
            const token = tokenizer.next();

            try tokens.append(gpa, .{
                .tag = token.tag,
                .start = @intCast(token.loc.start),
            });
            if (token.tag == .eoc) break;
        }

        return .{
            .gpa = gpa,
            .tokens = tokens.toOwnedSlice(),
            .source = source,
            .token_tags = undefined,
            .token_starts = undefined,
        };
    }

    pub fn deinit(self: *Parser_) void {
        self.tokens.deinit(self.gpa);
    }

    fn from_source(self: *Parser_) []const u8 {
        const start = self.token_starts[self.tok_i];
        const end = self.token_starts[self.tok_i + 1];
        return self.source[start..end];
    }

    pub fn parse(self: *Parser_) !Command_ {
        self.token_tags = self.tokens.items(.tag);
        self.token_starts = self.tokens.items(.start);

        for (self.token_starts) |start| {
            std.debug.print("START VAL: {d}\n", .{start});
        }

        for (self.token_tags) |tag| {
            std.debug.print("TAG VAL: {s}\n", .{@tagName(tag)});
        }

        const tag = self.token_tags[self.next_token()];

        switch (tag) {
            .asterisk => {
                _ = self.next_token(); // consume number token
                _ = self.next_token(); // consume carriage return token
                _ = self.next_token(); // consume dollar token
                _ = self.next_token(); // consume number token
                _ = self.next_token(); // consume carriage return token

                const cmd_string = self.from_source();
                std.debug.print("CMD STR: {s}\n", .{cmd_string});

                if (std.ascii.indexOfIgnoreCase(cmd_string, "ping")) |_| {
                    _ = self.next_token(); // consume string_token
                    _ = self.next_token(); // consume carriage return token

                    const cmd = Command_{ .ping = {} };

                    return cmd;
                }

                if (std.ascii.indexOfIgnoreCase(cmd_string, "echo")) |_| {
                    _ = self.next_token(); // consume string_token
                    _ = self.next_token(); // consume carriage return token

                    var cmd = Command_{ .echo = undefined };
                    _ = self.next_token(); // consume dollar token
                    _ = self.next_token(); // consume number token
                    _ = self.next_token(); // consume carriage return token

                    const echo_msg = self.from_source();
                    _ = self.next_token(); // consume carriage return token

                    std.debug.print("ECHO_MSG: {d}\n", .{echo_msg});

                    cmd.echo = echo_msg;
                    _ = self.next_token(); // consume carriage return token

                    return cmd;
                }

                if (std.ascii.indexOfIgnoreCase(cmd_string, "set")) |_| {
                    _ = self.next_token(); // consume string_token
                    _ = self.next_token(); // consume carriage return token

                    var cmd = Command_{ .set = .{
                        .key = undefined,
                        .val = undefined,
                        .exp = 0,
                    } };

                    _ = self.next_token(); // consume dollar token
                    _ = self.next_token(); // consume number token
                    _ = self.next_token(); // consume carriage return token

                    cmd.set.key = self.from_source();
                    _ = self.next_token(); // consume string_token

                    _ = self.next_token(); // consume carriage return token

                    _ = self.next_token(); // consume dollar token
                    _ = self.next_token(); // consume number token

                    _ = self.next_token(); // consume carriage return token

                    cmd.set.val = self.from_source();

                    _ = self.next_token(); // consume carriage return token

                    return cmd;
                }

                if (std.ascii.indexOfIgnoreCase(cmd_string, "get")) |_| {
                    _ = self.next_token(); // consume string_token
                    _ = self.next_token(); // consume carriage return token

                    var cmd = Command_{ .get = .{
                        .key = undefined,
                    } };

                    _ = self.next_token(); // consume dollar token
                    _ = self.next_token(); // consume number token
                    _ = self.next_token(); // consume carriage return token

                    cmd.get.key = self.from_source();
                    _ = self.next_token(); // consume string_token
                    _ = self.next_token(); // consume carriage return token

                    return cmd;
                }
            },
            else => {},
        }

        return error.UnableToParseCommand;
    }
};

test "parsing echo command" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);
    defer parser.deinit();

    const cmd = try parser.parse();

    const TestEnum = enum { phil, cool };
    const Test_ = struct { tag: TestEnum, start: u32 };
    std.debug.print("SIZE OF enum: {d}, ALIGNMENT: {d}\n", .{ @sizeOf(Test_), @alignOf(Test_) });

    try std.testing.expectEqual(Command_.echo, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "pineapple", cmd.echo);
}

test "parsing set command" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);
    defer parser.deinit();

    const cmd = try parser.parse();

    try std.testing.expectEqual(Command_.set, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "apple", cmd.set.key);
    try std.testing.expectEqualSlices(u8, "pear", cmd.set.val);
}
test "parsing get command" {
    const bytes = "*2\r\n$3\r\nGET\r\n$5\r\napple\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);
    defer parser.deinit();

    const cmd = try parser.parse();

    try std.testing.expectEqual(Command_.get, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "apple", cmd.get.key);
}

test "parsing ping command" {
    const bytes = "*1\r\n$4\r\nPING\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);
    defer parser.deinit();

    const cmd = try parser.parse();

    try std.testing.expectEqual(Command_.ping, std.meta.activeTag(cmd));
}

pub const Parser = struct {
    buffer: [:0]const u8,
    curr_index: usize,
    allocator: std.mem.Allocator,
    byte_count: usize = 0,

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
            var cmd = try self.parse();
            cmd.byte_count = self.byte_count;

            try cmds.append(cmd);

            self.byte_count = 0; // reset
        }

        return cmds;
    }

    pub fn parse(self: *Parser) !Command {
        var command = Command{ .loc = Loc{ .start = undefined, .end = undefined }, .tag = undefined, .args = undefined };
        // bytes sent from client ex: "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n"

        if (self.peek() == '*') {
            self.next();
        } else {
            // always count the first byte regardless of its type
            self.byte_count += 1;
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

            if (std.ascii.indexOfIgnoreCase(self.buffer[command.loc.start .. command.loc.end + 1], "wait")) |_| {
                command.tag = Tag.wait;
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

                    try self.expect_return_new_line_bytes();

                    command.args[i] = arg;

                    i += 1;
                }

                return command;
            },
            Tag.wait => {
                command.args[0] = try self.parse_string();

                try self.expect_return_new_line_bytes();

                command.args[1] = try self.parse_string();

                try self.expect_return_new_line_bytes();

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
                    arg.loc.end = self.curr_index;

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

        self.byte_count += 1;
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

test "test WAIT" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    const bytes = "*3\r\n$4\r\nWAIT\r\n$1\r\n5\r\n$3\r\n500\r\n";
    var parser = Parser.init(allocator, bytes);
    const command = try parser.parse();
    try std.testing.expectEqual(Tag.wait, command.tag);
    try std.testing.expectEqualSlices(u8, "5", command.args[0].content);
    try std.testing.expectEqualSlices(u8, "500", command.args[1].content);
}

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

    const bytes_three = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n";

    var parser_three = Parser.init(allocator, bytes_three);
    const command_three = try parser_three.parse();

    try std.testing.expectEqual(Tag.replconf, command_three.tag);
    try std.testing.expectEqualSlices(u8, "ACK", command_three.args[0].content);
    try std.testing.expectEqualSlices(u8, "0", command_three.args[1].content);
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
