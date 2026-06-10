#!lua name=gnode_asset

--
-- gNode ASSET Functions
-- A ValKey function library for asset storage, manifests, and bundle management
--
-- Assets are addressable content blobs (HTML, CSS, JS, JSON, etc.) with metadata.
-- Manifests describe how to assemble assets into bundles (compressed aggregates).
-- The Rust daemon reads manifests and builds bundles in a background loop.
--
-- Key patterns (all under {site_id}:asset):
--   {site_id}:asset:{asset_id}              — STRING: content (optionally compressed)
--   {site_id}:asset:{asset_id}:meta         — HASH: content_type, size, compressed, stored_at, version, etag
--   {site_id}:asset:manifests               — SET: registered manifest IDs
--   {site_id}:asset:manifest:{manifest_id}  — HASH: manifest definition fields
--   {site_id}:gnode:bundle:{manifest_id}      — STRING: gzip-compressed bundle output (written by daemon)
--   {site_id}:gnode:bundle:{manifest_id}:meta — HASH: built_at, size, compressed_size, asset_count
--
-- Usage:
--   GNODE_ASSET_STORE(keys, asset_id, content, content_type, ttl, site_id)
--   GNODE_ASSET_GET(keys, asset_id, site_id)
--   GNODE_ASSET_DELETE(keys, asset_id, site_id)
--   GNODE_ASSET_EXISTS(keys, asset_id, site_id)
--   GNODE_ASSET_LIST(keys, site_id, [content_type_filter])
--   GNODE_ASSET_MANIFEST_SET(keys, manifest_id, manifest_json, site_id)
--   GNODE_ASSET_MANIFEST_GET(keys, manifest_id, site_id)
--   GNODE_ASSET_MANIFEST_DELETE(keys, manifest_id, site_id)
--   GNODE_ASSET_MANIFEST_LIST(keys, site_id)
--   GNODE_ASSET_BUILD_STATUS(keys, manifest_id, site_id)
--

-- Safe JSON encode helper (pcall-wrapped per project convention)
local function safe_json_encode(value)
    local ok, json = pcall(cjson.encode, value)
    if not ok then return nil, "encode_error" end
    return json
end

-- Safe JSON decode helper
local function safe_json_decode(str)
    if not str then return nil, "nil_input" end
    local ok, data = pcall(cjson.decode, str)
    if not ok then return nil, "decode_error" end
    return data
end

-- Build a properly namespaced key
-- Rules:
--   Key starts with {site_id}: -> return as-is (already has hash tag)
--   Key starts with site_id: (no braces) -> add hash tag braces
--   Otherwise -> add {site_id}:asset: prefix
local function build_key(key, site_id)
    if not site_id or site_id == "" then
        site_id = "default"
    end

    -- Case 1: Already has hash tag with site_id
    if key:find("^{" .. site_id .. "}") then
        return key
    end

    -- Case 2: Key starts with site_id: (no braces)
    local site_prefix = site_id .. ":"
    if key:sub(1, #site_prefix) == site_prefix then
        return "{" .. site_id .. "}:" .. key:sub(#site_prefix + 1)
    end

    -- Case 3: Generic key without site prefix
    return '{' .. site_id .. '}:asset:' .. key
end

-- Track metrics for asset operations
local function track_metric(site_id, metric, value)
    local metrics_key = '{' .. site_id .. '}:metrics:asset'
    server.call('HINCRBY', metrics_key, metric, value or 1)
end


-- ============================================================================
-- GNODE_ASSET_STORE
-- Store an asset with content and metadata
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_STORE',
    description = 'Store an asset with content and metadata',
    callback = function(keys, args)
        -- args: asset_id, content, content_type, ttl, site_id, [version], [compressed]
        if #args < 3 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_STORE 0 asset_id content content_type [ttl] [site_id] [version] [compressed]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local asset_id = args[1]
        local content = args[2]
        local content_type = args[3]
        local ttl = tonumber(args[4] or '0')
        local site_id = args[5] or "default"
        local version = args[6] or "1"
        local compressed = args[7] == "true" or args[7] == "1"

        if not asset_id or asset_id == "" then
            local err_json = safe_json_encode({ok = false, error = "asset_id is required"})
            return err_json or '{"ok":false,"error":"missing_asset_id"}'
        end

        local now = tonumber(server.call('TIME')[1])
        local content_key = '{' .. site_id .. '}:asset:' .. asset_id
        local meta_key = content_key .. ':meta'

        -- Store content
        server.call('SET', content_key, content)

        -- Apply TTL if specified (0 = no expiry)
        if ttl > 0 then
            server.call('EXPIRE', content_key, ttl)
            server.call('EXPIRE', meta_key, ttl)
        end

        -- Compute a simple content hash (ETag) using string length + first/last chars
        -- This is a lightweight fingerprint, not cryptographic
        local etag_raw = #content .. ":" .. content_type .. ":" .. version
        local etag = tostring(#content) .. "-" .. tostring(now)

        -- Store metadata HASH
        server.call('HSET', meta_key,
            'id', asset_id,
            'ct', content_type,
            'sz', tostring(#content),
            'cmp', compressed and '1' or '0',
            'sa', tostring(now),
            'ua', tostring(now),
            'v', version,
            'etag', etag)

        -- Track metrics
        track_metric(site_id, 'stores', 1)
        track_metric(site_id, 'total_bytes_stored', #content)

        local result = {
            ok = true,
            asset_id = asset_id,
            key = content_key,
            size = #content,
            content_type = content_type,
            version = version,
            etag = etag,
            stored_at = now
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}


-- ============================================================================
-- GNODE_ASSET_GET
-- Retrieve an asset with content and metadata
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_GET',
    description = 'Retrieve an asset with content and metadata',
    callback = function(keys, args)
        -- args: asset_id, site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_GET 0 asset_id [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local asset_id = args[1]
        local site_id = args[2] or "default"

        local content_key = '{' .. site_id .. '}:asset:' .. asset_id
        local meta_key = content_key .. ':meta'

        -- Get content
        local content = server.call('GET', content_key)
        if not content then
            track_metric(site_id, 'misses', 1)
            local err_json = safe_json_encode({ok = false, error = "Asset not found: " .. asset_id})
            return err_json or '{"ok":false,"error":"not_found"}'
        end

        -- Get metadata
        local meta_raw = server.call('HGETALL', meta_key)
        local meta = {}
        if meta_raw and #meta_raw > 0 then
            for i = 1, #meta_raw, 2 do
                meta[meta_raw[i]] = meta_raw[i + 1]
            end
        end

        track_metric(site_id, 'gets', 1)

        local result = {
            ok = true,
            asset_id = asset_id,
            content = content,
            content_type = meta['ct'] or 'application/octet-stream',
            size = tonumber(meta['sz'] or '0'),
            compressed = meta['cmp'] == '1',
            version = meta['v'] or '1',
            etag = meta['etag'] or '',
            stored_at = tonumber(meta['sa'] or '0'),
            updated_at = tonumber(meta['ua'] or '0')
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {}  -- Writes metrics via track_metric()
}


-- ============================================================================
-- GNODE_ASSET_DELETE
-- Delete an asset, its metadata, and remove from any type indexes
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_DELETE',
    description = 'Delete an asset and its metadata',
    callback = function(keys, args)
        -- args: asset_id, site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_DELETE 0 asset_id [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local asset_id = args[1]
        local site_id = args[2] or "default"

        local content_key = '{' .. site_id .. '}:asset:' .. asset_id
        local meta_key = content_key .. ':meta'

        -- Check existence
        local existed = server.call('EXISTS', content_key)

        -- Delete content and metadata
        server.call('DEL', content_key)
        server.call('DEL', meta_key)

        if existed == 1 then
            track_metric(site_id, 'deletes', 1)
        end

        local result = {
            ok = true,
            asset_id = asset_id,
            deleted = existed == 1
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}


-- ============================================================================
-- GNODE_ASSET_EXISTS
-- Check if an asset exists
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_EXISTS',
    description = 'Check if an asset exists',
    callback = function(keys, args)
        -- args: asset_id, site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_EXISTS 0 asset_id [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local asset_id = args[1]
        local site_id = args[2] or "default"

        local content_key = '{' .. site_id .. '}:asset:' .. asset_id
        local exists = server.call('EXISTS', content_key)

        local result = {
            ok = true,
            asset_id = asset_id,
            exists = exists == 1
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}


-- ============================================================================
-- GNODE_ASSET_LIST
-- List assets for a site, optionally filtered by content type
-- Uses HSCAN on meta keys to avoid full keyspace scan
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_LIST',
    description = 'List assets for a site with optional content type filter',
    callback = function(keys, args)
        -- args: site_id, [content_type_filter], [cursor], [count]
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_LIST 0 site_id [content_type_filter] [cursor] [count]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local site_id = args[1]
        local ct_filter = args[2]  -- optional content_type filter
        local cursor = args[3] or '0'
        local count = tonumber(args[4] or '100')

        -- Normalize empty strings to nil
        if ct_filter == "" then ct_filter = nil end

        -- Scan for asset meta keys
        local pattern = '{' .. site_id .. '}:asset:*:meta'
        local scan_result = server.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', count)

        local next_cursor = scan_result[1]
        local meta_keys = scan_result[2]

        local assets = {}
        for _, meta_key in ipairs(meta_keys) do
            -- Read metadata fields we need
            local meta_raw = server.call('HMGET', meta_key, 'id', 'ct', 'sz', 'sa', 'v', 'etag', 'cmp')
            local id = meta_raw[1]
            local ct = meta_raw[2]
            local sz = meta_raw[3]
            local sa = meta_raw[4]
            local v = meta_raw[5]
            local etag = meta_raw[6]
            local cmp = meta_raw[7]

            -- Apply content type filter if specified
            local include = true
            if ct_filter and ct ~= ct_filter then
                include = false
            end

            if include and id then
                table.insert(assets, {
                    id = id,
                    content_type = ct or 'application/octet-stream',
                    size = tonumber(sz or '0'),
                    stored_at = tonumber(sa or '0'),
                    version = v or '1',
                    etag = etag or '',
                    compressed = cmp == '1'
                })
            end
        end

        local result = {
            ok = true,
            assets = assets,
            count = #assets,
            cursor = next_cursor,
            has_more = next_cursor ~= '0'
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}


-- ============================================================================
-- GNODE_ASSET_MANIFEST_SET
-- Store or update a bundle manifest and register it in the site's manifest index
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_MANIFEST_SET',
    description = 'Store or update a bundle manifest definition',
    callback = function(keys, args)
        -- args: manifest_id, manifest_json, site_id
        if #args < 2 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_MANIFEST_SET 0 manifest_id manifest_json [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local manifest_id = args[1]
        local manifest_json = args[2]
        local site_id = args[3] or "default"

        if not manifest_id or manifest_id == "" then
            local err_json = safe_json_encode({ok = false, error = "manifest_id is required"})
            return err_json or '{"ok":false,"error":"missing_manifest_id"}'
        end

        -- Validate JSON structure
        local manifest, decode_err = safe_json_decode(manifest_json)
        if not manifest then
            local err_json = safe_json_encode({ok = false, error = "Invalid manifest JSON: " .. (decode_err or "parse_error")})
            return err_json or '{"ok":false,"error":"invalid_json"}'
        end

        -- Validate required fields
        if not manifest.layout then
            local err_json = safe_json_encode({ok = false, error = "Manifest must include 'layout' field"})
            return err_json or '{"ok":false,"error":"missing_layout"}'
        end

        local now = tonumber(server.call('TIME')[1])
        local manifest_key = '{' .. site_id .. '}:asset:manifest:' .. manifest_id
        local index_key = '{' .. site_id .. '}:asset:manifests'

        -- Check if updating existing
        local existing = server.call('EXISTS', manifest_key)
        local is_update = existing == 1

        -- Serialize complex fields as JSON strings for HASH storage
        local slots_json = safe_json_encode(manifest.slots or {}) or '[]'
        local sections_json = safe_json_encode(manifest.sections or {}) or '{}'
        local build_options_json = safe_json_encode(manifest.build_options or {}) or '{}'

        -- Store manifest as HASH
        server.call('HSET', manifest_key,
            'id', manifest_id,
            'type', manifest.type or 'inline',
            'v', manifest.version or '1.0.0',
            'layout', manifest.layout,
            'sc', tostring(manifest.slot_count or 0),
            'slots', slots_json,
            'sections', sections_json,
            'bo', build_options_json,
            'ca', is_update and (server.call('HGET', manifest_key, 'ca') or tostring(now)) or tostring(now),
            'ua', tostring(now))

        -- Register in manifest index SET
        server.call('SADD', index_key, manifest_id)

        track_metric(site_id, 'manifest_writes', 1)

        local result = {
            ok = true,
            manifest_id = manifest_id,
            updated = is_update,
            layout = manifest.layout,
            slot_count = manifest.slot_count or 0,
            stored_at = now
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}


-- ============================================================================
-- GNODE_ASSET_MANIFEST_GET
-- Retrieve a manifest definition
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_MANIFEST_GET',
    description = 'Retrieve a bundle manifest definition',
    callback = function(keys, args)
        -- args: manifest_id, site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_MANIFEST_GET 0 manifest_id [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local manifest_id = args[1]
        local site_id = args[2] or "default"

        local manifest_key = '{' .. site_id .. '}:asset:manifest:' .. manifest_id

        -- Get all manifest fields
        local raw = server.call('HGETALL', manifest_key)
        if not raw or #raw == 0 then
            local err_json = safe_json_encode({ok = false, error = "Manifest not found: " .. manifest_id})
            return err_json or '{"ok":false,"error":"not_found"}'
        end

        -- Convert flat array to table
        local fields = {}
        for i = 1, #raw, 2 do
            fields[raw[i]] = raw[i + 1]
        end

        -- Parse JSON sub-fields back into objects
        local slots = safe_json_decode(fields['slots'] or '[]') or {}
        local sections = safe_json_decode(fields['sections'] or '{}') or {}
        local build_options = safe_json_decode(fields['bo'] or '{}') or {}

        local result = {
            ok = true,
            manifest = {
                id = fields['id'] or manifest_id,
                type = fields['type'] or 'inline',
                version = fields['v'] or '1.0.0',
                layout = fields['layout'],
                slot_count = tonumber(fields['sc'] or '0'),
                slots = slots,
                sections = sections,
                build_options = build_options,
                created_at = tonumber(fields['ca'] or '0'),
                updated_at = tonumber(fields['ua'] or '0')
            }
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}


-- ============================================================================
-- GNODE_ASSET_MANIFEST_DELETE
-- Delete a manifest and remove from the site's manifest index
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_MANIFEST_DELETE',
    description = 'Delete a bundle manifest and remove from index',
    callback = function(keys, args)
        -- args: manifest_id, site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_MANIFEST_DELETE 0 manifest_id [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local manifest_id = args[1]
        local site_id = args[2] or "default"

        local manifest_key = '{' .. site_id .. '}:asset:manifest:' .. manifest_id
        local index_key = '{' .. site_id .. '}:asset:manifests'
        local bundle_key = '{' .. site_id .. '}:gnode:bundle:' .. manifest_id
        local bundle_meta_key = bundle_key .. ':meta'

        -- Check existence
        local existed = server.call('EXISTS', manifest_key)

        -- Delete manifest HASH
        server.call('DEL', manifest_key)

        -- Remove from index SET
        server.call('SREM', index_key, manifest_id)

        -- Also delete built bundle if exists (stale without manifest)
        server.call('DEL', bundle_key)
        server.call('DEL', bundle_meta_key)

        if existed == 1 then
            track_metric(site_id, 'manifest_deletes', 1)
        end

        local result = {
            ok = true,
            manifest_id = manifest_id,
            deleted = existed == 1
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}


-- ============================================================================
-- GNODE_ASSET_MANIFEST_LIST
-- List all manifest IDs registered for a site
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_MANIFEST_LIST',
    description = 'List all bundle manifest IDs for a site',
    callback = function(keys, args)
        -- args: site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_MANIFEST_LIST 0 site_id"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local site_id = args[1]
        local index_key = '{' .. site_id .. '}:asset:manifests'

        -- Get all manifest IDs from SET
        local manifest_ids = server.call('SMEMBERS', index_key)

        -- Optionally fetch summary info for each manifest
        local manifests = {}
        for _, mid in ipairs(manifest_ids) do
            local manifest_key = '{' .. site_id .. '}:asset:manifest:' .. mid
            local fields = server.call('HMGET', manifest_key, 'layout', 'sc', 'v', 'ua')
            table.insert(manifests, {
                id = mid,
                layout = fields[1] or 'unknown',
                slot_count = tonumber(fields[2] or '0'),
                version = fields[3] or '1.0.0',
                updated_at = tonumber(fields[4] or '0')
            })
        end

        local result = {
            ok = true,
            manifests = manifests,
            count = #manifests
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}


-- ============================================================================
-- GNODE_ASSET_BUILD_STATUS
-- Get the build status/metadata for a manifest's built bundle
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ASSET_BUILD_STATUS',
    description = 'Get build status and metadata for a manifest bundle',
    callback = function(keys, args)
        -- args: manifest_id, site_id
        if #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_ASSET_BUILD_STATUS 0 manifest_id [site_id]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local manifest_id = args[1]
        local site_id = args[2] or "default"

        local bundle_key = '{' .. site_id .. '}:gnode:bundle:' .. manifest_id
        local bundle_meta_key = bundle_key .. ':meta'
        local manifest_key = '{' .. site_id .. '}:asset:manifest:' .. manifest_id

        -- Check if bundle exists
        local bundle_exists = server.call('EXISTS', bundle_key)
        local manifest_exists = server.call('EXISTS', manifest_key)

        if manifest_exists == 0 then
            local err_json = safe_json_encode({ok = false, error = "Manifest not found: " .. manifest_id})
            return err_json or '{"ok":false,"error":"not_found"}'
        end

        -- Get bundle metadata
        local meta_raw = server.call('HGETALL', bundle_meta_key)
        local meta = {}
        if meta_raw and #meta_raw > 0 then
            for i = 1, #meta_raw, 2 do
                meta[meta_raw[i]] = meta_raw[i + 1]
            end
        end

        -- Get manifest updated_at to check staleness
        local manifest_ua = tonumber(server.call('HGET', manifest_key, 'ua') or '0')
        local bundle_built_at = tonumber(meta['ba'] or '0')
        local is_stale = bundle_built_at < manifest_ua

        -- Get bundle TTL
        local ttl = -1
        if bundle_exists == 1 then
            ttl = server.call('TTL', bundle_key)
        end

        local result = {
            ok = true,
            manifest_id = manifest_id,
            built = bundle_exists == 1,
            stale = is_stale,
            built_at = bundle_built_at,
            size = tonumber(meta['sz'] or '0'),
            compressed_size = tonumber(meta['csz'] or '0'),
            asset_count = tonumber(meta['ac'] or '0'),
            builder_version = meta['bv'] or 'unknown',
            ttl = ttl
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}
