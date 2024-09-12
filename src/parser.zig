const Command = @import("type.zig").Command;
const Arg = @import("type.zig").Arg;
const Loc = @import("type.zig").Loc;
const Tag = @import("type.zig").Tag;
const RedisStore = @import("store.zig").RedisStore;
const std = @import("std");

pub const Command_ = union(enum) {
    ping,
    info: Info,
    set: Set,
    get: Get,
    psync: Psync,
    replconf: Replconf,
    wait: Wait,
    echo: Echo,
    config: Config,

    const Config = union(enum) { get: []const u8 };

    const Echo = []const u8;

    const Wait = struct { num_replicas_to_ack: usize, exp: i64 };

    const Info = enum { replication };

    const Set = struct {
        key: []const u8,
        val: []const u8,
        px: ?i64 = null,
    };
    const Get = struct {
        key: []const u8,
    };

    const Replconf = union(enum) {
        listening_port,
        capa_psync2,
        ack: usize,
        getack: []const u8,
    };

    const Psync = struct { replication_id: []const u8, offset: isize };
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
    source: [:0]const u8,

    pub fn init(gpa: std.mem.Allocator, source: [:0]const u8) !Parser_ {
        return .{
            .gpa = gpa,
            .source = source,
        };
    }

    pub fn parse_(self: *Parser_) !Command_ {
        var cmd_iter = std.mem.splitSequence(u8, self.source, "\r\n");

        if (cmd_iter.next()) |maybe_type| {
            const part = maybe_type[0];
            switch (part) {
                '*' => {
                    _ = cmd_iter.next(); // consume cmd length token

                    const cmd_string = cmd_iter.next().?; // consume cmd string

                    if (std.ascii.eqlIgnoreCase(cmd_string, "config")) {
                        _ = cmd_iter.next(); // consume length token
                        const subcommand = cmd_iter.next().?; // consume subcommand

                        if (std.ascii.eqlIgnoreCase(subcommand, "get")) {
                            _ = cmd_iter.next(); // consume length token
                            const config_param = cmd_iter.next().?; // consume the config value

                            return Command_{ .config = .{ .get = config_param } };
                        }
                    }
                    if (std.ascii.eqlIgnoreCase(cmd_string, "psync")) {
                        _ = cmd_iter.next(); // consume psync rep id length token

                        const rep_id = cmd_iter.next().?; // consume psync rep id token

                        _ = cmd_iter.next(); // consume wait psync offset length token

                        const offset = try std.fmt.parseInt(isize, cmd_iter.next().?, 10); // consume psync offset val

                        return Command_{ .psync = .{
                            .replication_id = rep_id,
                            .offset = offset,
                        } };
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "wait")) {
                        _ = cmd_iter.next(); // consume set key length token

                        const num = try std.fmt.parseInt(usize, cmd_iter.next().?, 10); // consume num rep ack val

                        _ = cmd_iter.next(); // consume wait val length token
                        const exp = try std.fmt.parseInt(i64, cmd_iter.next().?, 10); // consume wait val

                        return Command_{ .wait = .{
                            .num_replicas_to_ack = num,
                            .exp = exp,
                        } };
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "set")) {
                        _ = cmd_iter.next(); // consume set key length token

                        const key = cmd_iter.next().?; // consume key val
                        _ = cmd_iter.next(); // consume set val length token
                        const val = cmd_iter.next().?; // consume val

                        var cmd = Command_{ .set = .{
                            .key = key,
                            .val = val,
                        } };

                        if (cmd_iter.peek() != null and cmd_iter.peek().?.len > 0) {
                            _ = cmd_iter.next(); // consume set px len token
                            _ = cmd_iter.next(); // consume set px token
                            _ = cmd_iter.next(); // consume set px val length token

                            cmd.set.px = try std.fmt.parseInt(i16, cmd_iter.next().?, 10); // consume px val token
                        }

                        return cmd;
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "get")) {
                        _ = cmd_iter.next(); // consume set key length token
                        const key = cmd_iter.next().?; // consume key val

                        return Command_{ .get = .{
                            .key = key,
                        } };
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "echo")) {
                        _ = cmd_iter.next(); // consume set key length token
                        const val = cmd_iter.next().?; // consume val
                        return Command_{ .echo = val };
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "ping")) {
                        return Command_{ .ping = {} };
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "info")) {
                        _ = cmd_iter.next(); // consume length token
                        _ = cmd_iter.next(); // consume replication val
                        return Command_{ .info = .replication };
                    }

                    if (std.ascii.eqlIgnoreCase(cmd_string, "replconf")) {
                        _ = cmd_iter.next(); // consume length token
                        const subcommand = cmd_iter.next().?; // consume subcommand

                        if (std.ascii.eqlIgnoreCase(subcommand, "listening-port")) {
                            return Command_{ .replconf = .{ .listening_port = {} } };
                        } else if (std.ascii.eqlIgnoreCase(subcommand, "getack")) {
                            _ = cmd_iter.next(); // consume length token
                            const getack_asterisk = cmd_iter.next().?; // consume asterisk

                            return Command_{ .replconf = .{ .getack = getack_asterisk } };
                        } else if (std.ascii.eqlIgnoreCase(subcommand, "ack")) {
                            _ = cmd_iter.next(); // consume length token
                            const ack_val = try std.fmt.parseInt(usize, cmd_iter.next().?, 10); // consume ack val

                            return Command_{ .replconf = .{ .ack = ack_val } };
                        } else if (std.ascii.eqlIgnoreCase(subcommand, "capa")) {
                            _ = cmd_iter.next(); // consume length token
                            _ = cmd_iter.next(); // consume psync2 token

                            return Command_{ .replconf = .{ .capa_psync2 = {} } };
                        }
                    }
                },
                else => {},
            }
        }
        return error.UnableToParseCommand;
    }
};

test "parsing echo command" {
    const bytes = "*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.echo, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "pineapple", cmd.echo);
}

test "parsing config get command" {
    const bytes = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.config, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "dir", cmd.config.get);
}

test "parsing psync command" {
    const bytes = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.psync, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "?", cmd.psync.replication_id);
    try std.testing.expectEqual(-1, cmd.psync.offset);
}

test "parsing wait command" {
    const bytes = "*3\r\n$4\r\nWAIT\r\n$1\r\n5\r\n$3\r\n500\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.wait, std.meta.activeTag(cmd));
    try std.testing.expectEqual(5, cmd.wait.num_replicas_to_ack);
    try std.testing.expectEqual(500, cmd.wait.exp);
}

test "parsing set command" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.set, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "apple", cmd.set.key);
    try std.testing.expectEqualSlices(u8, "pear", cmd.set.val);
}

test "parsing set command with expiry option" {
    const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.set, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "apple", cmd.set.key);
    try std.testing.expectEqualSlices(u8, "pear", cmd.set.val);
    try std.testing.expectEqual(100, cmd.set.px.?);
}
test "parsing get command" {
    const bytes = "*2\r\n$3\r\nGET\r\n$5\r\napple\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.get, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "apple", cmd.get.key);
}

test "parsing ping command" {
    const bytes = "*1\r\n$4\r\nPING\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.ping, std.meta.activeTag(cmd));
}

test "parsing info command" {
    const bytes = "*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.info, std.meta.activeTag(cmd));
    try std.testing.expectEqual(Command_.Info.replication, cmd.info);
}

test "parsing replconf listening-port command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
    try std.testing.expectEqual({}, cmd.replconf.listening_port);
}

test "parsing replconf capa psync2 command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
    try std.testing.expectEqual({}, cmd.replconf.capa_psync2);
}

test "parsing replconf ack command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n5\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
    try std.testing.expectEqual(5, cmd.replconf.ack);
}

test "parsing replconf getack command" {
    const bytes = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    const gpa = std.testing.allocator;
    var parser = try Parser_.init(gpa, bytes);

    const cmd = try parser.parse_();

    try std.testing.expectEqual(Command_.replconf, std.meta.activeTag(cmd));
    try std.testing.expectEqualSlices(u8, "*", cmd.replconf.getack);
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

// test "test SET with expiry opt" {
//     const bytes = "*3\r\n$3\r\nSET\r\n$5\r\napple\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n";
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     defer _ = gpa.deinit();
//
//     const allocator = gpa.allocator();
//     var store = RedisStore.init(allocator);
//     defer store.deinit();
//
//     var parser = Parser.init(allocator, bytes);
//     const command = try parser.parse();
//     try store.set(command.args[0].content, command.args[1].content, command.opt.?.content);
//     try std.testing.expectEqual(Tag.set, command.tag);
//     try std.testing.expectEqualSlices(u8, "100", command.opt.?.content);
// }

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
