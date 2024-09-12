const std = @import("std");
const rand = std.crypto.random;
const net = std.net;
pub const Loc = struct { start: usize, end: usize };
pub const Tag = enum { echo, ping, set, get, info, replconf, psync, wait };
pub const Command = struct { loc: Loc, tag: Tag, args: [3]Arg, opt: ?Arg = null, byte_count: usize = 0 };
pub const Arg = struct { loc: Loc, tag: Tag, content: []const u8 };
pub const Role = enum { master, slave };

pub const Replica = struct {
    stream: net.Stream,
    allocator: std.mem.Allocator,
    offset: usize,

    pub fn init(allocator: std.mem.Allocator, stream: net.Stream) !*Replica {
        const replica = try allocator.create(Replica);
        replica.stream = stream;
        replica.allocator = allocator;
        replica.offset = 0;

        return replica;
    }

    pub fn destroy(self: *Replica) void {
        self.allocator.destroy(self);
    }

    pub fn write(self: *Replica, cmd_buf: []const u8) !void {
        _ = try self.stream.write(cmd_buf);
    }
};

// TODO: refactor ServerState, perhaps into a tagged union, that can differentiate between a master ServerState and a slave ServerState
pub const ServerState = struct {
    replicas: std.ArrayList(*Replica), // TODO: to deprecate
    replicas_2: std.AutoHashMap(std.posix.fd_t, *Replica),
    role: Role = .master,
    replication_id: ?[]u8 = null,
    master_host: ?[]const u8 = null,
    master_port: ?u16 = null,
    port: u16 = 6379,
    allocator: std.mem.Allocator,
    cmd_bytes_count: ?usize = null,
    offset: usize = 0,
    dir: []const u8 = undefined,
    dbfilename: []const u8 = undefined,

    pub fn init(allocator: std.mem.Allocator) ServerState {
        const replicas_2 = std.AutoHashMap(std.posix.fd_t, *Replica).init(allocator);
        return .{ .allocator = allocator, .replicas = std.ArrayList(*Replica).init(allocator), .replicas_2 = replicas_2 };
    }

    pub fn deinit(self: *ServerState) void {
        if (self.replication_id != null) {
            self.allocator.free(self.replication_id.?);
        }

        for (self.replicas.items) |replica| {
            replica.destroy();
        }

        self.replicas_2.deinit();

        self.replicas.deinit();
    }

    pub fn generate_master_replication_id(self: *ServerState) !void {
        var rep_id_slice = try self.allocator.alloc(u8, 40);
        var i: usize = 0;

        while (i < rep_id_slice.len) {
            const rand_int = rand.int(u8);

            if (std.ascii.isAlphanumeric(rand_int)) {
                rep_id_slice[i] = rand_int;
                i += 1;
            }
        }
        self.replication_id = rep_id_slice;
    }

    //TODO: to deprecate
    pub fn forward_cmd(self: *ServerState, cmd_buf: []const u8) !void {
        for (self.replicas.items) |replica| {
            try replica.write(cmd_buf);
        }
    }

    pub fn forward_cmd_2(self: *ServerState, cmd_buf: []const u8) !void {
        var iter = self.replicas_2.valueIterator();
        while (iter.next()) |replica_ptr| {
            try replica_ptr.*.write(cmd_buf);
        }
    }

    //TODO: to deprecate
    pub fn add_replica(self: *ServerState, stream: net.Stream) !void {
        const rep_ptr = try Replica.init(self.allocator, stream);
        try self.replicas.append(rep_ptr);
    }

    pub fn add_replica_2(self: *ServerState, stream: net.Stream) !void {
        const rep_ptr = try Replica.init(self.allocator, stream);
        try self.replicas_2.put(stream.handle, rep_ptr);
    }
};
