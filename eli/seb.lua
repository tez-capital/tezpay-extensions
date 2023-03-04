--[[
Extension created with support from The Sebuh.net Tezos Baker DAO
Distributes SEB tokens in fixed rate to delegators

extensions: [
	{
		command: eli
		args: ["extensions/seb.lua"]
		configuration: {
			exchange_rate: 0.1,			  // exchange rate in SEB per 1 XTZ
			ignore_delegators: [ ... ],	  // ignore delegators with these addresses
			ignore_smart_contracts: true, // ignore delegators that are smart contracts
			contract_address: "KT1...",   // address of the SEB contract
			token_id: 0,				  // token id of the SEB token
			token_symbol: "SEB",		  // token symbol used in messages
		}
	}
]
]]
local def                            = {
	configuration = {
		exchange_rate = 0.5,
		ignore_delegators = {},
		ignore_smart_contracts = true,
		--log_file = "seb.log", -- uncomment for default log file
		token_symbol = "SEB"
	}
}

local hjson                          = require("hjson")

-- calls
local CALL_PREFIX                    = "tp."
local CLOSE_CALL                     = "close"
local INIT_CALL                      = "init"
local HEALTHCHECK_CALL               = "healthcheck"
-- hooks
local TEST_REQUEST_HOOK              = "test-request"
local AFTER_CANDIDATE_GENERATED_HOOK = "after_candidate_generated"
local AFTER_BONDS_DISTRIBUTED_HOOK   = "after_bonds_distributed"
local CHECK_BALANCE_HOOK             = "check_balance"
local ON_FEES_COLLECTION_HOOK        = "on_fees_collection"


local METHOD_NOT_FOUND = { code = -32601, message = "Method not found" }
local INVALID_REQUEST = { code = -32600, message = "Invalid request" }

local function new_server_error(data)
	local SERVER_ERROR = { code = -32000, message = "Server error", data = data }
	return SERVER_ERROR
end

local function validate_configuration()
	if def.configuration.contract_address == nil then
		return new_server_error("contract_address is not set")
	end
	if def.configuration.token_id == nil then
		return new_server_error("token_id is not set")
	end
	if type(def.configuration.exchange_rate) ~= "number" then
		return new_server_error("exchange_rate is not a number")
	end
	if not util.is_array(def.configuration.ignore_delegators) then
		return new_server_error("ignore_delegators is not an array")
	end
	if type(def.configuration.ignore_smart_contracts) ~= "boolean" then
		return new_server_error("ignore_smart_contracts is not a boolean")
	end
	if def.kind ~= "stdio" then
		return new_server_error("seb extension only supports stdio kind")
	end

	local validHookCombinationChecks = {
		function(hooks)
			return table.includes(hooks, "all:rw") and not table.includes(hooks, AFTER_BONDS_DISTRIBUTED_HOOK .. ":ro") and
				not table.includes(hooks, CHECK_BALANCE_HOOK .. ":ro")
		end,
		function(hooks)
			return table.includes(hooks, AFTER_BONDS_DISTRIBUTED_HOOK .. ":rw") and
				table.includes(hooks, CHECK_BALANCE_HOOK .. ":rw")
		end
	}
	local hasValidCombinationOfHooks = false
	for _, check in ipairs(validHookCombinationChecks) do
		if check(def.hooks) then
			hasValidCombinationOfHooks = true
		end
	end
	if not hasValidCombinationOfHooks then
		return new_server_error("seb extension requires rw access the following hooks: " ..
		AFTER_BONDS_DISTRIBUTED_HOOK .. ", " .. CHECK_BALANCE_HOOK)
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

local function stringify(value)
	return hjson.stringify_to_json(value, { indent = false })
end

local function write_error(id, error)
	local response = stringify({ jsonrpc = "2.0", id = id, error = METHOD_NOT_FOUND })
	log("ERROR: " .. response)
	io.write(response .. "\n")
	io.output():flush()
end

local function write_response(id, result)
	local response = stringify({ jsonrpc = "2.0", id = id, result = result })
	log("response: " .. response)
	io.write(response .. "\n")
	io.output():flush()
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
	[AFTER_BONDS_DISTRIBUTED_HOOK] = function(request)
		local id = request.id
		local version = request.params.version
		if version ~= "0.1" then
			write_error(id, new_server_error("Unsupported version: " .. version))
			return
		end

		local candidates = request.params.data
		local result = util.clone(candidates, true)
		for _, candidate in ipairs(candidates) do
			if def.configuration.ignore_smart_contracts and candidate.recipient:find("KT1") == 1 then
				goto CONTINUE
			end
			if table.includes(def.configuration.ignore_delegators, candidate.source) then
				goto CONTINUE
			end
			if candidate.is_invalid then
				goto CONTINUE
			end
			local bonds_amount = bigint.new(candidate.bonds_amount)
			if bonds_amount <= 0 then
				goto CONTINUE
			end

			local faCandidate = util.clone(candidate, true)
			local percent = def.configuration.exchange_rate * 100
			faCandidate.bonds_amount = tostring((bonds_amount * percent) / 100)
			faCandidate.tx_kind = "fa2"
			faCandidate.fa_contract = def.configuration.contract_address
			faCandidate.fa_token_id = tostring(def.configuration.token_id)

			table.insert(result, faCandidate)
			::CONTINUE::
		end

		write_response(id, result)
	end,
	[CHECK_BALANCE_HOOK] = function(request)
		local hookData = request.params.data
		local version = request.params.version
		if version ~= "0.1" then
			write_error(request.id, new_server_error("Unsupported version: " .. version))
			return
		end
		local totalSeb = 0
		local payouts = hookData.payouts or {}
		for _, payout in ipairs(payouts) do
			if payout.is_invalid then
				goto CONTINUE
			end
			if payout.tx_kind == "fa2" and payout.fa_contract == def.configuration.contract_address and payout.fa_token_id == tostring(def.configuration.token_id) then
				local amount = payout.amount and tonumber(payout.amount) or 0
				totalSeb = totalSeb + tonumber(amount)
			end
			::CONTINUE::
		end

		local url = string.interpolate(
			"https://api.tzkt.io/v1/tokens/balances?account=${addr}&token.tokenId=${tokeId}&token.contract=${contract}&limit=1&select=balance",
			{
				addr = def.payoutPkh,
				tokeId = def.configuration.token_id,
				contract = def.configuration.contract_address,
			})
		local ok, data = net.safe_download_string(url)
		if not ok then
			write_error(request.id,
				new_server_error("Failed to check " ..
				tostring(def.configuration.token_symbol) .. " balance: " .. tostring(data)))
			return
		end

		local result = hookData
		result.is_sufficient = true
		local balances = hjson.parse(data)
		if #balances < 1 then
			result.is_sufficient = false
			result.message = "Insufficient " .. tostring(def.configuration.token_symbol) .. " balance"
		elseif tonumber(balances[1]) < totalSeb then
			result.is_sufficient = false
			result.message = "Insufficient " .. tostring(def.configuration.token_symbol) .. " balance"
		end

		write_response(request.id, result)
	end
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
