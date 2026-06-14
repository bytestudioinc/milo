require("dotenv").config();
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(cors());

// Health check endpoint
app.get("/health", (req, res) => {
  res.send({ status: "healthy", timestamp: new Date() });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const PORT = process.env.PORT || 8080;

// Initialize Supabase Client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
let supabase = null;
if (supabaseUrl && supabaseServiceKey) {
  supabase = createClient(supabaseUrl, supabaseServiceKey);
  console.log("Supabase Service Client initialized successfully.");
} else {
  console.warn("WARNING: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY not configured. Paid matchmaking will fail validation.");
}

// Queues
// Each user in the queue is represented by:
// {
//   socketId: string,
//   deviceId: string,
//   name: string,
//   gender: string,
//   country: string,
//   filterGender: string, // "anyone", "male", "female"
//   filterCountry: string, // "Global", or specific country
//   cost: number,
//   joinedAt: number
// }
let freeQueue = [];
let paidQueue = [];

// Active calls mapping: roomId -> session
const activeCalls = new Map();

// Helper mappings for socket/user tracking
const socketToUser = new Map(); // socket.id -> { deviceId }
const userSockets = new Map();   // deviceId -> socket.id
const disconnectTimeouts = new Map(); // deviceId -> Timeout

// Find active call room for a device
function findActiveCallRoom(deviceId) {
  for (const [roomId, session] of activeCalls.entries()) {
    if (session.caller.deviceId === deviceId || session.callee.deviceId === deviceId) {
      return roomId;
    }
  }
  return null;
}

// Clean up call states
async function terminateCall(roomId, disconnectedDeviceId, reason) {
  const session = activeCalls.get(roomId);
  if (!session) return;

  console.log(`[CALL] Terminating call ${roomId}. Reason: ${reason}`);
  session.status = "ended";

  const peer = session.caller.deviceId === disconnectedDeviceId ? session.callee : session.caller;
  const peerSocketId = userSockets.get(peer.deviceId);
  
  if (peerSocketId) {
    io.to(peerSocketId).emit("peer_disconnected", { reason });
  }

  const duration = (Date.now() - session.startTime) / 1000;
  console.log(`[CALL] Call duration was ${duration}s.`);

  // Clear tracking
  userSockets.delete(session.caller.deviceId);
  userSockets.delete(session.callee.deviceId);
  activeCalls.delete(roomId);
}

// Handle socket disconnections during calls
function handleCallDisconnect(deviceId, roomId) {
  console.log(`[SOCKET] User ${deviceId} socket disconnected from active call ${roomId}. Waiting 5s for reconnect...`);

  // Set timeout to terminate call if not reconnected in 5 seconds
  const timeoutId = setTimeout(async () => {
    disconnectTimeouts.delete(deviceId);
    console.log(`[SOCKET] User ${deviceId} failed to reconnect within grace period. Ending call.`);
    await terminateCall(roomId, deviceId, "disconnect_timeout");
  }, 5000);

  disconnectTimeouts.set(deviceId, timeoutId);
}

io.on("connection", (socket) => {
  console.log(`Socket connected: ${socket.id}`);

  // Enqueue handler (supports both join_queue and legacy enqueue)
  const handleJoinQueue = async (userData) => {
    console.log(`join_queue requested: ${userData.name} (${socket.id})`, userData);

    // Clean up if already enqueued
    freeQueue = freeQueue.filter((u) => u.deviceId !== userData.deviceId);
    paidQueue = paidQueue.filter((u) => u.deviceId !== userData.deviceId);

    const filterGender = userData.filterGender || "anyone";
    const filterCountry = userData.filterCountry || "Global";
    const isPaid = (filterGender !== "anyone") || (filterCountry !== "Global");

    let cost = 0;

    if (isPaid) {
      if (!supabase) {
        socket.emit("queue_rejected", { error: "Paid matching is temporarily unavailable." });
        return;
      }
      if (!userData.token) {
        socket.emit("queue_rejected", { error: "Authentication token required for paid matching." });
        return;
      }

      try {
        // 1. Verify token
        const { data: { user: authUser }, error: authError } = await supabase.auth.getUser(userData.token);
        if (authError || !authUser) {
          socket.emit("queue_rejected", { error: "Session expired. Please log in again." });
          return;
        }

        // 2. Fetch balance and config costs
        const [userRes, configRes] = await Promise.all([
          supabase.from("users").select("gem_balance").eq("auth_user_id", authUser.id).single(),
          supabase.from("app_config").select("match_gender_cost, match_location_cost").single()
        ]);

        if (userRes.error || !userRes.data) {
          socket.emit("queue_rejected", { error: "Could not fetch user profile." });
          return;
        }
        if (configRes.error || !configRes.data) {
          socket.emit("queue_rejected", { error: "Could not load system cost configuration." });
          return;
        }

        const balance = userRes.data.gem_balance || 0;
        const genderCost = configRes.data.match_gender_cost ?? 5;
        const locationCost = configRes.data.match_location_cost ?? 5;

        if (filterGender !== "anyone") cost += genderCost;
        if (filterCountry !== "Global") cost += locationCost;

        if (balance < cost) {
          socket.emit("queue_rejected", { error: "Insufficient gems for selected preferences." });
          return;
        }
        
        console.log(`[QUEUE] Validation passed. Cost: ${cost} gems. Balance: ${balance} gems.`);
      } catch (err) {
        console.error("[QUEUE] Token validation error:", err);
        socket.emit("queue_rejected", { error: "Validation failed." });
        return;
      }
    }

    const user = {
      socketId: socket.id,
      deviceId: userData.deviceId,
      name: userData.name,
      gender: userData.gender || "Rather not say",
      country: userData.country || "Global",
      filterGender: filterGender,
      filterCountry: filterCountry,
      cost: cost,
      joinedAt: Date.now(),
    };

    socketToUser.set(socket.id, { deviceId: user.deviceId });
    userSockets.set(user.deviceId, socket.id);

    if (isPaid) {
      paidQueue.push(user);
      console.log(`[QUEUE] Added ${user.name} to Paid Queue.`);
      socket.emit("queue_status", { status: "queued", position: paidQueue.length });
    } else {
      freeQueue.push(user);
      console.log(`[QUEUE] Added ${user.name} to Free Queue.`);
      socket.emit("queue_status", { status: "queued", position: freeQueue.length });
    }

    tryMatchQueue();
  };

  socket.on("join_queue", handleJoinQueue);
  socket.on("enqueue", handleJoinQueue); // Backward compatibility

  // Dequeue handler (supports both leave_queue and legacy dequeue)
  const handleLeaveQueue = () => {
    console.log(`leave_queue / dequeue requested: ${socket.id}`);
    const userInfo = socketToUser.get(socket.id);
    if (userInfo) {
      const { deviceId } = userInfo;
      freeQueue = freeQueue.filter((u) => u.deviceId !== deviceId);
      paidQueue = paidQueue.filter((u) => u.deviceId !== deviceId);
    }
    socket.emit("queue_status", { status: "idle" });
  };

  socket.on("leave_queue", handleLeaveQueue);
  socket.on("dequeue", handleLeaveQueue); // Backward compatibility

  // Signaling message relay (routes signals by active call rooms)
  socket.on("signal", (data) => {
    const userInfo = socketToUser.get(socket.id);
    if (userInfo) {
      const { deviceId } = userInfo;
      const roomId = findActiveCallRoom(deviceId);
      if (roomId) {
        const session = activeCalls.get(roomId);
        if (session) {
          const peer = session.caller.deviceId === deviceId ? session.callee : session.caller;
          const peerSocketId = userSockets.get(peer.deviceId);
          if (peerSocketId) {
            io.to(peerSocketId).emit("signal", {
              sender: socket.id,
              ...data,
            });
          }
        }
      }
    }
  });

  // Re-associate socket to active call on reconnection
  socket.on("reconnect_call", (data) => {
    const { roomId, deviceId } = data;
    console.log(`[SOCKET] User ${deviceId} reconnected on new socket ID: ${socket.id}`);

    const session = activeCalls.get(roomId);
    if (!session) {
      socket.emit("call_reconnect_failed", { error: "Call session no longer exists." });
      return;
    }

    // Cancel the disconnect timeout if running
    const timeoutId = disconnectTimeouts.get(deviceId);
    if (timeoutId) {
      clearTimeout(timeoutId);
      disconnectTimeouts.delete(deviceId);
      console.log(`[SOCKET] Reconnection verified. Discarded disconnect timeout for device: ${deviceId}`);
    }

    // Re-bind mapping
    socketToUser.set(socket.id, { deviceId });
    userSockets.set(deviceId, socket.id);

    // Update session
    if (session.caller.deviceId === deviceId) {
      session.caller.socketId = socket.id;
    } else if (session.callee.deviceId === deviceId) {
      session.callee.socketId = socket.id;
    }

    socket.emit("call_reconnected", { roomId });
    console.log(`[SOCKET] Reconnection successfully established for device: ${deviceId}`);
  });

  // Hangup call
  socket.on("hangup", async () => {
    const userInfo = socketToUser.get(socket.id);
    if (userInfo) {
      const { deviceId } = userInfo;
      const activeRoomId = findActiveCallRoom(deviceId);
      if (activeRoomId) {
        await terminateCall(activeRoomId, deviceId, "hangup");
      }
    }
  });

  // Handle disconnection
  socket.on("disconnect", () => {
    console.log(`Socket disconnected: ${socket.id}`);
    const userInfo = socketToUser.get(socket.id);
    if (userInfo) {
      const { deviceId } = userInfo;
      
      // Remove from queues
      freeQueue = freeQueue.filter((u) => u.deviceId !== deviceId);
      paidQueue = paidQueue.filter((u) => u.deviceId !== deviceId);

      // Check if they were in an active call
      const activeRoomId = findActiveCallRoom(deviceId);
      if (activeRoomId) {
        handleCallDisconnect(deviceId, activeRoomId);
      }
    }
    socketToUser.delete(socket.id);
  });
});

// Check filtering compatibility
function isCompatible(a, b) {
  // 1. Check Gender filters
  const aGenderMatch =
    a.filterGender === "All" ||
    a.filterGender === "anyone" ||
    b.gender === "Rather not say" ||
    b.gender.toLowerCase() === a.filterGender.toLowerCase();

  const bGenderMatch =
    b.filterGender === "All" ||
    b.filterGender === "anyone" ||
    a.gender === "Rather not say" ||
    a.gender.toLowerCase() === b.filterGender.toLowerCase();

  if (!aGenderMatch || !bGenderMatch) return false;

  // 2. Check Country filters
  const aCountryMatch =
    a.filterCountry === "Global" ||
    b.country.toLowerCase().includes(a.filterCountry.toLowerCase()) ||
    a.filterCountry.toLowerCase().includes(b.country.toLowerCase());

  const bCountryMatch =
    b.filterCountry === "Global" ||
    a.country.toLowerCase().includes(b.filterCountry.toLowerCase()) ||
    b.filterCountry.toLowerCase().includes(a.country.toLowerCase());

  if (!aCountryMatch || !bCountryMatch) return false;

  return true;
}

// Matchmaking algorithm
function tryMatchQueue() {
  // 1. First process Paid Queue
  if (paidQueue.length > 0) {
    paidQueue.sort((a, b) => a.joinedAt - b.joinedAt);

    for (let i = 0; i < paidQueue.length; i++) {
      const userA = paidQueue[i];

      // Try matching with someone in the paid queue first
      for (let j = 0; j < paidQueue.length; j++) {
        if (i === j) continue;
        const userB = paidQueue[j];

        if (isCompatible(userA, userB)) {
          matchUsers(userA, userB);
          return tryMatchQueue();
        }
      }

      // Try matching with someone in the free queue
      freeQueue.sort((a, b) => a.joinedAt - b.joinedAt);
      for (let j = 0; j < freeQueue.length; j++) {
        const userB = freeQueue[j];

        if (isCompatible(userA, userB)) {
          matchUsers(userA, userB);
          return tryMatchQueue();
        }
      }
    }
  }

  // 2. Process remaining Free Queue
  if (freeQueue.length >= 2) {
    freeQueue.sort((a, b) => a.joinedAt - b.joinedAt);

    for (let i = 0; i < freeQueue.length; i++) {
      const userA = freeQueue[i];

      for (let j = i + 1; j < freeQueue.length; j++) {
        const userB = freeQueue[j];

        if (isCompatible(userA, userB)) {
          matchUsers(userA, userB);
          return tryMatchQueue();
        }
      }
    }
  }
}

// Perform matchmaking and notify users
function matchUsers(userA, userB) {
  paidQueue = paidQueue.filter((u) => u.deviceId !== userA.deviceId && u.deviceId !== userB.deviceId);
  freeQueue = freeQueue.filter((u) => u.deviceId !== userA.deviceId && u.deviceId !== userB.deviceId);

  const roomId = `room_${userA.deviceId}_${userB.deviceId}`;

  const session = {
    roomId,
    caller: {
      socketId: userA.socketId,
      deviceId: userA.deviceId,
      name: userA.name,
      gender: userA.gender,
      country: userA.country,
      cost: userA.cost,
    },
    callee: {
      socketId: userB.socketId,
      deviceId: userB.deviceId,
      name: userB.name,
      gender: userB.gender,
      country: userB.country,
      cost: userB.cost,
    },
    startTime: Date.now(),
    status: "connecting",
  };

  activeCalls.set(roomId, session);

  console.log(`[MATCH] Match found: ${userA.name} <-> ${userB.name} in Room ${roomId}`);

  io.to(userA.socketId).emit("matched", {
    room: roomId,
    peer: {
      name: userB.name,
      gender: userB.gender,
      country: userB.country,
    },
    isInitiator: true,
  });

  io.to(userB.socketId).emit("matched", {
    room: roomId,
    peer: {
      name: userA.name,
      gender: userA.gender,
      country: userA.country,
    },
    isInitiator: false,
  });
}

server.listen(PORT, () => {
  console.log(`Milo matchmaking server listening on port ${PORT}`);
});
