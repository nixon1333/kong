local endpoints = require "kong.api.endpoints"
local utils = require "kong.tools.utils"


local escape_uri = ngx.escape_uri
local unescape_uri = ngx.unescape_uri
local null = ngx.null
local fmt = string.format


local function post_health(self, db, is_healthy)
  local upstream, _, err_t = endpoints.select_entity(self, db, db.upstreams.schema)
  if err_t then
    return endpoints.handle_error(err_t)
  end

  if not upstream then
    return endpoints.not_found()
  end

  local target, _, err_t
  if utils.is_valid_uuid(unescape_uri(self.params.targets)) then
    target, _, err_t = endpoints.select_entity(self, db, db.targets.schema)
    if err_t then
      return endpoints.handle_error(err_t)
    end
  end

  if not target then
    -- we did not find target by id, so it means we need to look it harder
    local opts = endpoints.extract_options(self.args.post, db.targets.schema, "select")
    local upstream_pk = db.upstreams.schema:extract_pk_values(upstream)
    target, _, err_t = db.targets:select_by_upstream_target(upstream_pk, unescape_uri(self.params.targets), opts)
    if err_t then
      return endpoints.handle_error(err_t)
    end

    if not target then
      return endpoints.not_found()
    end
  end

  local ok, err = db.targets:post_health(upstream, target, is_healthy)
  if not ok then
    local body = utils.get_default_exit_body(400, err)
    return endpoints.bad_request(body)
  end

  return endpoints.no_content()
end


return {
  ["/upstreams/:upstreams/health"] = {
    GET = function(self, db)
      local upstream, _, err_t = endpoints.select_entity(self, db, db.upstreams.schema)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      if not upstream then
        return endpoints.not_found()
      end

      local upstream_pk = db.upstreams.schema:extract_pk_values(upstream)
      local args = self.args.uri
      local size, err = endpoints.get_page_size(args)
      if err then
        return endpoints.handle_error(db.targets.errors:invalid_size(err))
      end

      local opts = endpoints.extract_options(args, db.targets.schema, "select")
      local targets_with_health, _, err_t, offset =
        db.targets:page_for_upstream_with_health(upstream_pk, size, args.offset, opts)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      local next_page = offset and fmt("/upstreams/%s/health?offset=%s",
                                       self.params.upstreams,
                                       escape_uri(offset)) or null

      local node_id, err = kong.node.get_id()
      if err then
        kong.log.err("failed getting node id: ", err)
      end

      return endpoints.ok {
        data    = targets_with_health,
        offset  = offset,
        next    = next_page,
        node_id = node_id,
      }
    end
  },

  ["/upstreams/:upstreams/targets"] = {
    GET = endpoints.get_collection_endpoint(kong.db.targets.schema,
                                            kong.db.upstreams.schema,
                                            "upstream",
                                            "page_for_upstream_without_inactive"),
  },

  ["/upstreams/:upstreams/targets/all"] = {
    GET = function(self, db)
      local upstream, _, err_t = endpoints.select_entity(self, db, db.upstreams.schema)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      if not upstream then
        return endpoints.not_found()
      end

      local upstream_pk = db.upstreams.schema:extract_pk_values(upstream)
      local args = self.args.uri
      local opts = endpoints.extract_options(args, db.targets.schema, "select")
      local size, err = endpoints.get_page_size(args)
      if err then
        return endpoints.handle_error(db.targets.errors:invalid_size(err))
      end

      local targets, _, err_t, offset = db.targets:page_for_upstream(upstream_pk,
                                                                     size,
                                                                     args.offset,
                                                                     opts)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      local next_page = offset and fmt("/upstreams/%s/targets/all?offset=%s",
                                       self.params.upstreams,
                                       escape_uri(offset)) or null

      return endpoints.ok {
        data   = targets,
        offset = offset,
        next   = next_page,
      }
    end
  },

  ["/upstreams/:upstreams/targets/:targets/healthy"] = {
    POST = function(self, db)
      return post_health(self, db, true)
    end,
  },

  ["/upstreams/:upstreams/targets/:targets/unhealthy"] = {
    POST = function(self, db)
      return post_health(self, db, false)
    end,
  },

  ["/upstreams/:upstreams/targets/:targets"] = {
    DELETE = function(self, db)
      local upstream, _, err_t = endpoints.select_entity(self, db, db.upstreams.schema)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      if not upstream then
        return endpoints.not_found()
      end

      local target, _, err_t
      if utils.is_valid_uuid(unescape_uri(self.params.targets)) then
        target, _, err_t = endpoints.select_entity(self, db, db.targets.schema)
        if err_t then
          return endpoints.handle_error(err_t)
        end
      end

      if not target then
        -- we did not find target by id, so it means we need to look it harder
        local opts = endpoints.extract_options(nil, db.targets.schema, "select")
        local upstream_pk = db.upstreams.schema:extract_pk_values(upstream)
        target, _, err_t = db.targets:select_by_upstream_target(upstream_pk, unescape_uri(self.params.targets), opts)
        if err_t then
          return endpoints.handle_error(err_t)
        end

        if not target then
          return endpoints.not_found()
        end
      end

      local opts = endpoints.extract_options(nil, db.targets.schema, "delete")
      local _, _, err_t = db.targets:delete({ id = target.id }, opts)
      if err_t then
        return endpoints.handle_error(err_t)
      end

      return endpoints.no_content()
    end
  },
}

