--[[
NOTE: DO NOT USE. THIS IS A WORK IN PROGRESS.
Distributes accumulated rewards after a preconfigured minimum payout is reached

extensions: [
    {
        command: eli
        args: ["extensions/acc.lua"]
        configuration: {
            min_payout: 100,          // minimum payout amount in XTZ - precision to 3 decimal places (e.g. 0.001 XTZ)
            accumulated_rewards_file: "accumulated_rewards.json", // file to store accumulated rewards
        }
    }
]
]]
local def = {
    configuration = {
        min_payout = 100,
        accumulated_rewards_file = "accumulated_rewards.json",
		log_file = "acc.log",
    }
}

local hjson = require("hjson")
local bigint = require("bigint")

-- calls
local CALL_PREFIX = "tp."
local CLOSE_CALL = "close"
local INIT_CALL = "init"
local HEALTHCHECK_CALL = "healthcheck"
-- hooks
local TEST_REQUEST_HOOK              = "test-request"
local ON_FEES_COLLECTION_HOOK        = "on_fees_collection"

local METHOD_NOT_FOUND = { code = -32601, message = "Method not found" }
local INVALID_REQUEST = { code = -32600, message = "Invalid request" }

local function new_server_error(data)
    local SERVER_ERROR = { code = -32000, message = "Server error", data = data }
    return SERVER_ERROR
end

local function validate_configuration()
	if def.kind ~= "stdio" then
		return new_server_error("acc extension only supports stdio kind")
	end
	if def.configuration.min_payout == nil or bigint.new(def.configuration.min_payout * 1000) * 1000 < 0 then
		return new_server_error("min_payout must be a string representation of a non-negative number")
	end

	local validHookCombinationChecks = {
		function(hooks)
			return table.includes(hooks, "all:rw") and not table.includes(hooks, ON_FEES_COLLECTION_HOOK .. ":ro")
		end,
		function(hooks)
			return table.includes(hooks, ON_FEES_COLLECTION_HOOK .. ":rw")
		end
	}
	local hasValidCombinationOfHooks = false
	for _, check in ipairs(validHookCombinationChecks) do
		if check(def.hooks) then
			hasValidCombinationOfHooks = true
		end
	end
	if not hasValidCombinationOfHooks then
		return new_server_error("acc extension requires rw access to the following hook: " .. ON_FEES_COLLECTION_HOOK)
	end
end

local function validate_request(request)
    if type(request) ~= "table" then
        return INVALID_REQUEST
    end
    if type(request.method) ~= "string" then
        return INVALID_REQUEST
    end
    if request.method:sub(1, #CALL_PREFIX) ~= CALL_PREFIX then
        return new_server_error("Method must start with " .. CALL_PREFIX)
    end
end

local function log(message)
	if type(def.configuration.log_file) == "string" and #def.configuration.log_file > 0 then
		fs.write_file(def.configuration.log_file, tostring(message) .. "\n", { append = true })
	end
end

local function write_error(id, error)
	local response = hjson.stringify({ jsonrpc = "2.0", id = id, error = error })
	log("ERROR: " .. response)
	io.write(response .. "\n")
	io.output():flush()
end

local function write_response(id, result)
    local response = hjson.stringify_to_json({ jsonrpc = "2.0", id = id, result = result })
    io.write(response .. "\n")
    io.output():flush()
end

local function get_accumulated_rewards()
	if not fs.exists(def.configuration.accumulated_rewards_file) then
		return {}
	end
    local content = fs.read_file(def.configuration.accumulated_rewards_file)
    if content then
        return hjson.parse(content)
    else
        return {}
    end
end

local function update_accumulated_rewards(accumulated_rewards)
    local content = hjson.stringify_to_json(accumulated_rewards)
    local temp_file = def.configuration.accumulated_rewards_file .. ".tmp"

    -- Write content to a temporary file
    fs.write_file(temp_file, content)

    -- Check if the temporary file was created successfully
    if not os.rename(temp_file, def.configuration.accumulated_rewards_file) then
        error("Failed to create temporary file for atomic write")
    end
end

local handlers = {
	[INIT_CALL] = function(request)
		local id = request.id
		def = util.merge_tables(def, request.params.definition, true)
		def.bakerPkh = request.params.baker_pkh
		def.payoutPkh = request.params.payout_pkh
		local error = validate_configuration()
		if error ~= nil then
			write_error(id, error)
			write_response(id, {
				success = false,
				error = error,
			})
			os.exit(1)
			return
		end

		write_response(id, {
			success = true,
		})
	end,
	[CLOSE_CALL] = function()
		os.exit(0)
	end,
	[HEALTHCHECK_CALL] = function()
	end,
	[TEST_REQUEST_HOOK] = function(request)
		local data = request.params.data
		data.message = "Hello from Lua!"
		write_response(request.id, data)
	end,
	[ON_FEES_COLLECTION_HOOK] = function(request)
		local id = request.id
		local version = request.params.version
		if version ~= "0.2" then
			write_error(id, new_server_error("Unsupported version: " .. version))
			return
		end

		local data = request.params.data
		local candidates = data.candidates
		local result = {
			cycle = data.cycle,
			candidates = {}
		}

		-- we multiply by 1000 to maintain precision but avoiding overflow
		local min_payout = bigint.new(def.configuration.min_payout * 1000) * 1000 -- Convert min_payout from XTZ to mutez
		local accumulated_rewards = get_accumulated_rewards()

		for _, candidate in ipairs(candidates) do
			table.insert(result.candidates, candidate)
			if candidate.is_invalid then
				goto CONTINUE
			end

			local reward_amount = bigint.new(candidate.bonds_amount)
			if reward_amount < min_payout then
				local source = candidate.source
				local previous_reward = accumulated_rewards[source]
				if previous_reward then
					reward_amount = reward_amount + bigint.new(previous_reward)
				end

				if reward_amount >= min_payout then
					candidate.bonds_amount = tostring(reward_amount)
					accumulated_rewards[source] = nil
				else
					accumulated_rewards[source] = tostring(reward_amount)
					candidate.bonds_amount = "0" -- Set candidate.bonds_amount to zero
					candidate.is_invalid = true
					candidate.invalid_because = "PAYOUT_BELLOW_MINIMUM * ACCOUNTED"
					goto CONTINUE
				end
			end

			::CONTINUE::
		end

		local ok, err = pcall(update_accumulated_rewards, accumulated_rewards)
		if ok then
			write_response(id, result)
		else
			write_error(id, new_server_error("Failed to update accumulated rewards: " .. tostring(err)))
		end
	end,
}

local function listen()
	while true do
		local line = io.read()
		if not line then
			break
		end
		log("request: " .. line)
		local request = hjson.parse(line)
		local error = validate_request(request)
		if error ~= nil then
			write_error(request.id, error)
			return
		end

		local id = request.id
		local method = request.method:sub(#CALL_PREFIX + 1)

		local handler = handlers[method]
		if handler ~= nil then
			handler(request)
		elseif id ~= nil then -- ignores notifications
			write_error(id, METHOD_NOT_FOUND)
		end
	end
end

local ok, error = pcall(listen)
if not ok then
	log("ERROR: " .. tostring(error))
end
