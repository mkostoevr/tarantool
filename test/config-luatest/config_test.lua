local t = require('luatest')
local server = require('luatest.server')
local helpers = require('test.config-luatest.helpers')
local treegen = require('luatest.treegen')
local justrun = require('luatest.justrun')
local yaml = require('yaml')
local json = require('json')
local fio = require('fio')

local g = t.group()

g.after_each(function()
    for k, v in pairs(g) do
        if k == 'server' or k:match('^server_%d+$') then
            v:stop()
        end
    end
end)

local function verify_configdata()
    local json = require('json')
    local t = require('luatest')
    local configdata = require('internal.config.configdata')
    local cluster_config = require('internal.config.cluster_config')

    local saved_assert_equals = t.assert_equals
    t.assert_equals = function(...)
        local ok, err = pcall(saved_assert_equals, ...)
        if not ok then
            error(json.encode(err), 2)
        end
    end

    local cconfig = {
        credentials = {
            users = {
                guest = {
                    roles = {'super'},
                },
            },
        },
        iproto = {
            listen = {{uri = 'unix/:./{{ instance_name }}.iproto'}},
        },
        groups = {
            ['group-001'] = {
                memtx = {
                    memory = 1000000,
                },
                replicasets = {
                    ['replicaset-001'] = {
                        sql = {
                            cache_size = 2000,
                        },
                        instances = {
                            ['instance-001'] = {
                                database = {
                                    mode = 'rw',
                                },
                            },
                            ['instance-002'] = {
                                database = {
                                    mode = 'ro',
                                },
                            },
                        },
                    },
                },
            },
        },
    }
    cluster_config:validate(cconfig)
    local iconfig = cluster_config:instantiate(cconfig, 'instance-001')
    local data = configdata.new(iconfig, cconfig, 'instance-001')
    local expected = {
        credentials = {
            users = {
                guest = {
                    roles = {'super'},
                },
            },
        },
        iproto = {
            listen = {{uri = 'unix/:./instance-001.iproto'}},
        },
        sql = {
            cache_size = 2000,
        },
        database = {
            mode = 'rw',
        },
        memtx = {
            memory = 1000000,
        },
    }
    t.assert_equals(data:get(), expected)

    t.assert_equals(data:get('iproto'), expected.iproto)

    local f = function(w) return w.schema.type == 'integer' end
    local function remove_descriptions(tbl)
        if type(tbl) ~= 'table' then
            return
        end

        for key, value in pairs(tbl) do
            if key == 'description' then
                tbl[key] = nil
            elseif type(value) == 'table' then
                remove_descriptions(value)
            end
        end
    end

    local filtered_data = data:filter(f):totable()
    remove_descriptions(filtered_data)
    local expected_filtered_data = {
        {
            data = 2000,
            path = {"sql", "cache_size"},
            schema = {
                box_cfg = "sql_cache_size",
                default = 5242880,
                type = "integer",
                computed = {
                    annotations = {
                        box_cfg = "sql_cache_size",
                    },
                },
            },
        },
        {
            data = 1000000,
            path = {"memtx", "memory"},
            schema = {
                box_cfg = "memtx_memory",
                default = 268435456,
                type = "integer",
                computed = {
                    annotations = {
                        box_cfg = "memtx_memory",
                    },
                },
            },
        },
    }
    t.assert_items_equals(filtered_data, expected_filtered_data)

    local f2 = function(w) return w.path[#w.path], w.data end
    local mapped_data = data:filter(f):map(f2):tomap()
    local expected_filtered_mapped_data = {
        cache_size = 2000,
        memory = 1000000,
    }
    t.assert_equals(mapped_data, expected_filtered_mapped_data)

    local expected_names = {
        group_name = "group-001",
        instance_name = "instance-001",
        replicaset_name = "replicaset-001",
    }
    local res_names = data:names()
    t.assert_equals(res_names, expected_names)

    t.assert_equals(data:peers(), {'instance-001', 'instance-002'})
end

g.test_configdata = function()
    local dir = treegen.prepare_directory({}, {})
    local script = string.dump(verify_configdata)
    treegen.write_file(dir, 'main.lua', script)

    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, {}, {'main.lua'}, opts)
    t.assert_equals(res.exit_code, 0, {res.stdout, res.stderr})
end

g.test_config_general = function()
    local dir = treegen.prepare_directory({}, {})
    local script = [[
        local json = require('json')
        local config = require('config')
        local file_config = "\
            log:\
              level: 7\
            memtx:\
              min_tuple_size: 16\
              memory: 100000000\
            groups:\
              group-001:\
                replicasets:\
                  replicaset-001:\
                    instances:\
                      instance-001: {}\
        "
        file = io.open('config.yaml', 'w')
        file:write(file_config)
        file:close()
        assert(config:info().status == 'uninitialized')
        config:_startup('instance-001', 'config.yaml')
        assert(config:info().status == 'ready')
        assert(box.cfg.memtx_min_tuple_size == 16)
        assert(box.cfg.memtx_memory == 100000000)
        assert(box.cfg.log_level == 0)
        local res = {old = config:info().alerts}

        file_config = "\
            log:\
              level: 7\
            memtx:\
              min_tuple_size: 32\
              memory: 100000001\
            groups:\
              group-001:\
                replicasets:\
                  replicaset-001:\
                    instances:\
                      instance-001: {}\
        "
        file = io.open('config.yaml', 'w')
        file:write(file_config)
        file:close()
        config:reload()
        assert(box.cfg.memtx_min_tuple_size == 16)
        assert(box.cfg.memtx_memory == 100000001)
        assert(box.cfg.log_level == 0)
        res.new = config:info().alerts
        print(json.encode(res))
        os.exit(0)
    ]]
    treegen.write_file(dir, 'main.lua', script)

    local env = {TT_LOG_LEVEL = 0}
    local opts = {nojson = true, stderr = false}
    local res = justrun.tarantool(dir, env, {'main.lua'}, opts)
    t.assert_equals(res.exit_code, 0)
    local info = json.decode(res.stdout)
    t.assert_equals(info.old, {})
    t.assert_equals(#info.new, 1)
    t.assert_equals(info.new[1].type, 'warn')
    local exp = "box_cfg.apply: non-dynamic option memtx_min_tuple_size will "..
                "not be set until the instance is restarted"
    t.assert_equals(info.new[1].message, exp)
end

g.test_config_broadcast = function()
    local dir = treegen.prepare_directory({}, {})
    local file_config = [[
        app:
          file: 'script.lua'

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', file_config)

    local main = [[
        local fiber = require('fiber')
        local config = require('config')
        local status = ''
        box.watch('config.info', function(_, v) status = v.status end)
        config:_startup('instance-001', 'config.yaml')
        while status ~= 'ready' do
            fiber.sleep(0.1)
        end
        print(status)
        config:reload()
        while status ~= 'ready' do
            fiber.sleep(0.1)
        end
        print(status)
        os.exit(0)
    ]]
    treegen.write_file(dir, 'main.lua', main)

    local script = [[
        local fiber = require('fiber')
        local status = ''
        box.watch('config.info', function(_, v) status = v.status end)
        while not string.endswith(status, '_in_progress') do
            fiber.sleep(0.1)
        end
        print(status)
    ]]
    treegen.write_file(dir, 'script.lua', script)

    local opts = {nojson = true, stderr = false}
    local args = {'main.lua'}
    local res = justrun.tarantool(dir, {}, args, opts)
    t.assert_equals(res.exit_code, 0)
    local exp = {'startup_in_progress', 'ready', 'reload_in_progress', 'ready'}
    t.assert_equals(res.stdout, table.concat(exp, "\n"))
end

g.test_config_option = function()
    local dir = treegen.prepare_directory({}, {})
    local file_config = [[
        log:
          level: 7

        memtx:
          min_tuple_size: 16
          memory: 100000000

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', file_config)

    local script = [[
        print(box.cfg.memtx_min_tuple_size)
        print(box.cfg.memtx_memory)
        print(box.cfg.log_level)
        os.exit(0)
    ]]
    treegen.write_file(dir, 'main.lua', script)

    local env = {TT_LOG_LEVEL = 0}
    local opts = {nojson = true, stderr = false}
    local args = {'--name', 'instance-001', '--config', 'config.yaml',
                  'main.lua'}
    local res = justrun.tarantool(dir, env, args, opts)
    t.assert_equals(res.exit_code, 0)
    t.assert_equals(res.stdout, table.concat({16, 100000000, 0}, "\n"))
end

g.test_remaining_vinyl_options = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        vinyl:
          bloom_fpr: 0.37
          page_size: 777
          range_size: 5555
          run_count_per_level: 3
          run_size_ratio: 1.63
          read_threads: 11
          write_threads: 22
          cache: 111111111
          defer_deletes: true
          memory: 222222222
          timeout: 7.5

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.vinyl_bloom_fpr, 0.37)
        t.assert_equals(box.cfg.vinyl_page_size, 777)
        t.assert_equals(box.cfg.vinyl_range_size, 5555)
        t.assert_equals(box.cfg.vinyl_run_count_per_level, 3)
        t.assert_equals(box.cfg.vinyl_run_size_ratio, 1.63)
        t.assert_equals(box.cfg.vinyl_read_threads, 11)
        t.assert_equals(box.cfg.vinyl_write_threads, 22)
        t.assert_equals(box.cfg.vinyl_cache, 111111111)
        t.assert_equals(box.cfg.vinyl_defer_deletes, true)
        t.assert_equals(box.cfg.vinyl_memory, 222222222)
        t.assert_equals(box.cfg.vinyl_timeout, 7.5)
    end)
end

g.test_feedback_options = function()
    t.skip_if(box.internal.feedback_daemon == nil, 'Feedback is disabled')
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        feedback:
          crashinfo: false
          host: 'https://feedback.tarantool.io'
          metrics_collect_interval: 120
          send_metrics: false
          enabled: false
          interval: 7200
          metrics_limit: 1000000

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.feedback_crashinfo, false)
        t.assert_equals(box.cfg.feedback_host, 'https://feedback.tarantool.io')
        t.assert_equals(box.cfg.feedback_metrics_collect_interval, 120)
        t.assert_equals(box.cfg.feedback_send_metrics, false)
        t.assert_equals(box.cfg.feedback_enabled, false)
        t.assert_equals(box.cfg.feedback_interval, 7200)
        t.assert_equals(box.cfg.feedback_metrics_limit, 1000000)
    end)
end

g.test_memtx_sort_threads = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        memtx:
            sort_threads: 11

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.memtx_sort_threads, 11)
    end)

    config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        memtx:
            sort_threads: 12

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', config)
    g.server:exec(function()
        local config = require('config')
        config:reload()
        t.assert_equals(box.cfg.memtx_sort_threads, 11)
        t.assert_equals(#config:info().alerts, 1)
        local exp = "box_cfg.apply: non-dynamic option memtx_sort_threads "..
                    "will not be set until the instance is restarted"
        t.assert_equals(config:info().alerts[1].message, exp)
    end)
end

g.test_bootstrap_leader = function(g)
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        groups:
          group-001:
            replicasets:
              replicaset-001:
                bootstrap_leader: 'instance-001'
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local env = {TT_LOG_LEVEL = 0}
    local args = {'--name', 'instance-001', '--config', config_file}
    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, env, args, opts)
    local exp = 'LuajitError: The "bootstrap_leader" option cannot be set '..
                'for replicaset "replicaset-001" because '..
                '"bootstrap_strategy" for instance "instance-001" is not '..
                '"config"\nfatal error, exiting the event loop'
    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        replication:
          bootstrap_strategy: 'config'

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', config)
    res = justrun.tarantool(dir, env, args, opts)
    exp = 'LuajitError: The \"bootstrap_leader\" option cannot be empty for '..
          'replicaset "replicaset-001" because "bootstrap_strategy" for '..
          'instance "instance-001" is "config"'..
          '\nfatal error, exiting the event loop'
    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        replication:
          bootstrap_strategy: 'config'

        groups:
          group-001:
            replicasets:
              replicaset-001:
                bootstrap_leader: 'instance-002'
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', config)
    res = justrun.tarantool(dir, env, args, opts)
    exp = 'LuajitError: "bootstrap_leader" = "instance-002" option is set '..
          'for replicaset "replicaset-001" of group "group-001", but '..
          'instance "instance-002" is not found in this replicaset'..
          '\nfatal error, exiting the event loop'
    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        replication:
          bootstrap_strategy: 'config'

        groups:
          group-001:
            replicasets:
              replicaset-001:
                bootstrap_leader: 'instance-001'
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', config)

    opts = {config_file = config_file, alias = 'instance-001', chdir = dir}
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.bootstrap_leader, 'instance-001')
        t.assert_equals(box.cfg.bootstrap_strategy, 'config')
    end)
end

g.test_flightrec_options = function()
    t.tarantool.skip_if_not_enterprise()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        flightrec:
            enabled: false
            logs_log_level: 5
            logs_max_msg_size: 8192
            logs_size: 1000000
            metrics_interval: 5
            metrics_period: 240
            requests_max_req_size: 10000
            requests_max_res_size: 20000
            requests_size: 2000000

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.flightrec_enabled, false)
        t.assert_equals(box.cfg.flightrec_logs_log_level, 5)
        t.assert_equals(box.cfg.flightrec_logs_max_msg_size, 8192)
        t.assert_equals(box.cfg.flightrec_logs_size, 1000000)
        t.assert_equals(box.cfg.flightrec_metrics_interval, 5)
        t.assert_equals(box.cfg.flightrec_metrics_period, 240)
        t.assert_equals(box.cfg.flightrec_requests_max_req_size, 10000)
        t.assert_equals(box.cfg.flightrec_requests_max_res_size, 20000)
        t.assert_equals(box.cfg.flightrec_requests_size, 2000000)
    end)
end

g.test_security_options = function()
    t.tarantool.skip_if_not_enterprise()
    local dir = treegen.prepare_directory({}, {})
    -- guest user is required for luatest.server helper to function properly,
    -- so it is enabled (`disable_guest: false`) unlike other options.
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        security:
            auth_type: pap-sha256
            auth_delay: 5
            auth_retries: 3
            disable_guest: false
            secure_erasing: false
            password_lifetime_days: 90
            password_min_length: 14
            password_enforce_uppercase: true
            password_enforce_lowercase: true
            password_enforce_digits: true
            password_enforce_specialchars: true
            password_history_length: 3

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.auth_type, 'pap-sha256')
        t.assert_equals(box.cfg.auth_delay, 5)
        t.assert_equals(box.cfg.auth_retries, 3)
        t.assert_equals(box.cfg.disable_guest, false)
        t.assert_equals(box.cfg.secure_erasing, false)
        t.assert_equals(box.cfg.password_lifetime_days, 90)
        t.assert_equals(box.cfg.password_min_length, 14)
        t.assert_equals(box.cfg.password_enforce_uppercase, true)
        t.assert_equals(box.cfg.password_enforce_lowercase, true)
        t.assert_equals(box.cfg.password_enforce_digits, true)
        t.assert_equals(box.cfg.password_enforce_specialchars, true)
        t.assert_equals(box.cfg.password_history_length, 3)
    end)
end

g.test_metrics_options_default = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]

    -- Test defaults.
    local config_file = treegen.write_file(dir, 'base_config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.metrics.include, {'all'})
        t.assert_equals(box.cfg.metrics.exclude, { })
        t.assert_equals(box.cfg.metrics.labels, {alias = 'instance-001'})
    end)
end

g.test_metrics_options = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        metrics:
          include: [cpu]
          exclude: [all]
          labels:
            foo: bar

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]

    local config_file = treegen.write_file(dir, 'base_config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.metrics.include, {'cpu'})
        t.assert_equals(box.cfg.metrics.exclude, {'all'})
        t.assert_equals(box.cfg.metrics.labels, {foo = 'bar'})
    end)
end

g.test_metrics_1_3_0_options = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        metrics:
          include: [all]
          exclude: [cpu_extended]

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]

    local config_file = treegen.write_file(dir, 'base_config.yaml', config)
    local opts = {
        config_file = config_file,
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        t.assert_equals(box.cfg.metrics.include, {'all'})
        t.assert_equals(box.cfg.metrics.exclude, {'cpu_extended'})
    end)
end

g.test_audit_options = function()
    t.tarantool.skip_if_not_enterprise()
    local dir = treegen.prepare_directory({}, {})

    local events = {
        'audit_enable', 'custom', 'auth_ok', 'auth_fail', 'disconnect',
        'user_create', 'user_drop', 'role_create', 'role_drop', 'user_enable',
        'user_disable', 'user_grant_rights', 'user_revoke_rights',
        'role_grant_rights', 'role_revoke_rights', 'password_change',
        'access_denied', 'eval', 'call', 'space_select', 'space_create',
        'space_alter', 'space_drop', 'space_insert', 'space_replace',
        'space_delete', 'none', 'all', 'audit', 'auth', 'priv', 'ddl', 'dml',
        'data_operations', 'compatibility'
    }

    local verify = function(events)
        t.assert_equals(box.cfg.audit_log, nil)
        t.assert_equals(box.cfg.audit_nonblock, true)
        t.assert_equals(box.cfg.audit_format, 'csv')
        t.assert_equals(box.cfg.audit_filter, table.concat(events, ","))
        t.assert_equals(box.cfg.audit_spaces, {'space1', 'space2', 'space3'})
        t.assert_equals(box.cfg.audit_extract_key, true)
    end

    helpers.success_case(g, {
        dir = dir,
        options = {
            ['audit_log.to'] = 'devnull',
            ['audit_log.nonblock'] = true,
            ['audit_log.format'] = 'csv',
            ['audit_log.filter'] = events,
            ['audit_log.spaces'] = {'space1', 'space2', 'space3'},
            ['audit_log.extract_key'] = true,
        },
        verify = verify,
        verify_args = {events}
    })
end

-- "replication.failover" = "supervised" mode has several
-- constraints.
--
-- "<replicaset>.leader" and "database.mode" options are
-- forbidden.
--
-- "replication.bootstrap_strategy" = "auto", "supervised",
-- "native" are supported, but other strategies aren't.
g.test_failover_supervised_constrainsts = function()
    local replicaset_prefix = 'groups.group-001.replicasets.replicaset-001'
    local instance_prefix = replicaset_prefix .. '.instances.instance-001'

    local leader_exp_err = '"leader" = "instance-001" option is set for ' ..
        'replicaset "replicaset-001" of group "group-001", but this option ' ..
        'cannot be used together with replication.failover = "supervised"'
    local mode_exp_err = 'database.mode = "ro" is set for instance ' ..
        '"instance-001" of replicaset "replicaset-001" of group ' ..
        '"group-001", but this option cannot be used together with ' ..
        'replication.failover = "supervised"'
    local bootstrap_strategy_exp_err_template = '"bootstrap_strategy" = %q ' ..
        'is set for replicaset "replicaset-001", but it is not supported ' ..
        'with "replication.failover" = "supervised"'

    -- The "<replicaset>.leader" option is forbidden in the
    -- "supervised" failover mode.
    helpers.failure_case({
        options = {
            ['replication.failover'] = 'supervised',
            [replicaset_prefix .. '.leader'] = 'instance-001',
        },
        exp_err = leader_exp_err,
    })

    -- The "database.mode" option is forbidden in the "supervised"
    -- failover mode.
    helpers.failure_case({
        options = {
            ['replication.failover'] = 'supervised',
            [instance_prefix .. '.database.mode'] = 'ro',
        },
        exp_err = mode_exp_err
    })

    -- "replication.bootstrap_strategy" = "legacy" is forbidden.
    helpers.failure_case({
        options = {
            ['replication.failover'] = 'supervised',
            ['replication.bootstrap_strategy'] = 'legacy',
        },
        exp_err = bootstrap_strategy_exp_err_template:format('legacy'),
    })

    -- "replication.bootstrap_strategy" = "config" is forbidden.
    helpers.failure_case({
        options = {
            ['replication.failover'] = 'supervised',
            ['replication.bootstrap_strategy'] = 'config',
            [replicaset_prefix .. '.bootstrap_leader'] = 'instance-001',
        },
        exp_err = bootstrap_strategy_exp_err_template:format('config'),
    })
end

g.test_failover_config = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        failover:
          replicasets:
            replicaset-002: []

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
                  instance-002: {}
                  instance-003: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local env = {TT_LOG_LEVEL = 0}
    local args = {'--name', 'instance-001', '--config', config_file}
    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, env, args, opts)
    local exp = 'LuajitError: replicaset replicaset-002 specified in the ' ..
                'failover configuration doesn\'t exist\nfatal error, ' ..
                'exiting the event loop'

    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        failover:
          replicasets:
            replicaset-001:
              priority:
                instance-004: 1

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
                  instance-002: {}
                  instance-003: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local env = {TT_LOG_LEVEL = 0}
    local args = {'--name', 'instance-001', '--config', config_file}
    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, env, args, opts)
    local exp = 'LuajitError: instance instance-004 from replicaset ' ..
                'replicaset-001 specified in the failover.replicasets ' ..
                'section doesn\'t exist\nfatal error, exiting the event loop'

    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        failover:
          replicasets:
            replicaset-001:
              priority:
                instance-003: 1

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
                  instance-002: {}
              replicaset-002:
                instances:
                  instance-003: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local env = {TT_LOG_LEVEL = 0}
    local args = {'--name', 'instance-001', '--config', config_file}
    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, env, args, opts)
    local exp = 'LuajitError: instance instance-003 from replicaset ' ..
                'replicaset-002 is specified in the wrong replicaset ' ..
                'replicaset-001 in the failover.replicasets configuration '..
                'section\nfatal error, exiting the event loop'

    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        failover:
          replicasets:
            replicaset-001:
              learners:
                - instance-004

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
                  instance-002: {}
                  instance-003: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local env = {TT_LOG_LEVEL = 0}
    local args = {'--name', 'instance-001', '--config', config_file}
    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, env, args, opts)
    local exp = 'LuajitError: instance instance-004 from replicaset ' ..
                'replicaset-001 specified in the failover.replicasets ' ..
                'section doesn\'t exist\nfatal error, exiting the event loop'

    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)

    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        failover:
          replicasets:
            replicaset-001:
              learners:
                - instance-003

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
                  instance-002: {}
              replicaset-002:
                instances:
                  instance-003: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local env = {TT_LOG_LEVEL = 0}
    local args = {'--name', 'instance-001', '--config', config_file}
    local opts = {nojson = true, stderr = true}
    local res = justrun.tarantool(dir, env, args, opts)
    local exp = 'LuajitError: instance instance-003 from replicaset ' ..
                'replicaset-002 is specified in the wrong replicaset ' ..
                'replicaset-001 in the failover.replicasets configuration '..
                'section\nfatal error, exiting the event loop'

    t.assert_equals(res.exit_code, 1)
    t.assert_equals(res.stderr, exp)
end

g.test_failover_bootstrap_leader_considers_priorities = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        replication:
          failover: 'supervised'

        failover:
          replicasets:
            replicaset-001:
              priority:
                instance-002: 10
                instance-003: 10

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
                  instance-002: {}
                  instance-003: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)

    g.server_1 = server:new({ config_file = config_file, chdir = dir,
                              alias = 'instance-001' })
    g.server_2 = server:new({ config_file = config_file, chdir = dir,
                              alias = 'instance-002' })
    g.server_3 = server:new({ config_file = config_file, chdir = dir,
                              alias = 'instance-003' })

    g.server_1:start({wait_until_ready = false})
    g.server_2:start({wait_until_ready = false})
    g.server_3:start({wait_until_ready = false})

    g.server_1:wait_until_ready()
    g.server_2:wait_until_ready()
    g.server_3:wait_until_ready()

    g.server_1:exec(function()
        t.assert_equals(box.info.ro, true)
    end)

    g.server_2:exec(function()
        t.assert_equals(box.info.ro, false)
    end)

    g.server_3:exec(function()
        t.assert_equals(box.info.ro, true)
    end)
end

g.test_advertise_from_env = function(g)
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    local opts = {
        config_file = config_file,
        chdir = dir,
        alias = 'instance-001',
        env = {
            TT_IPROTO_ADVERTISE_PEER_URI = 'unix/:./instance-001.iproto',
            TT_IPROTO_ADVERTISE_PEER_PARAMS_TRANSPORT = 'plain',
            TT_IPROTO_ADVERTISE_SHARDING_URI = 'unix/:./instance-002.iproto'
        }
    }
    g.server = server:new(opts)
    g.server:start()
    g.server:exec(function()
        local config = require('config')
        local res = config:get('iproto.advertise')
        local exp = {
            client = box.NULL,
            peer = {
                uri = 'unix/:./instance-001.iproto',
                params = {
                    transport = 'plain',
                },
            },
            sharding = {
                uri = 'unix/:./instance-002.iproto',
            }
        }
        t.assert_equals(res, exp)
    end)
end

g.test_listen_from_env = function()
    local dir = treegen.prepare_directory({}, {})
    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super
        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', config)
    local script = [[
        print(require('yaml').encode(box.cfg.listen))
        os.exit(0)
    ]]
    treegen.write_file(dir, 'main.lua', script)
    local listen = {
        {
            uri = 'unix/:./instance-001_1.iproto',
            params = {transport = 'plain'},
        },
        {
            uri = 'unix/:./instance-001_2.iproto',
            params = {transport = 'plain'},
        },
        {
            uri = 'unix/:./instance-001_3.iproto',
        },
    }
    local env = {TT_IPROTO_LISTEN = json.encode(listen)}
    local opts = {nojson = true, stderr = false}
    local args = {'--name', 'instance-001', '--config', 'config.yaml',
                  'main.lua'}
    local res = justrun.tarantool(dir, env, args, opts)
    t.assert_equals(res.exit_code, 0)
    t.assert_equals(yaml.decode(res.stdout), listen)
end

g.test_iproto_listen_plain = function()
    local dir = treegen.prepare_directory({}, {})

    local verify = function()
        local res = {
            {
                params = {transport = "plain"},
                uri = "unix/:./instance-001.iproto",
            },
            {
                params = {transport = "plain"},
                uri = "unix/:./instance-001_2.iproto",
            },
            {
                params = {transport = "plain"},
                uri = "unix/:./instance-001_3.iproto",
            },
        }
        t.assert_equals(box.cfg.listen, res)
    end

    helpers.success_case(g, {
        dir = dir,
        options = {
            ['iproto.listen'] = {
                {
                    uri = 'unix/:./{{ instance_name }}.iproto',
                    params = {transport = 'plain'},
                },
                {
                    uri = 'unix/:./{{ instance_name }}_2.iproto',
                    params = {transport = 'plain'},
                },
                {
                    uri = 'unix/:./{{ instance_name }}_3.iproto',
                    params = {transport = 'plain'},
                },
            },
        },
        verify = verify,
    })
end

g.test_iproto_listen_to_advertise = function()
    local dir = treegen.prepare_directory({}, {})
    local config = yaml.encode({
        credentials = {
            users = {
                guest = {
                    roles = {'super'},
                },
                replicator = {
                    roles = {'replication'},
                    password = 'secret',
                },
            },
        },
        iproto = {
            listen = {{
                uri = 'unix/:./{{ instance_name }}.iproto',
                params = {
                    transport = 'plain',
                },
            }},
            advertise = {
                peer = {
                    login = 'replicator',
                },
            },
        },
        groups = {
            ['group-001'] = {
                replicasets = {
                    ['replicaset-001'] = {
                        instances = {
                            ['instance-001'] = {
                                database = {
                                    mode = 'rw',
                                },
                            },
                            ['instance-002'] = {},
                        },
                    },
                },
            },
        },
    })
    local config_file = treegen.write_file(dir, 'config.yaml', config)
    g.server_1 = server:new({config_file = config_file, chdir = dir,
                             alias = 'instance-001'})
    g.server_2 = server:new({config_file = config_file, chdir = dir,
                             alias = 'instance-002'})

    g.server_1:start({wait_until_ready = false})
    g.server_2:start({wait_until_ready = false})

    g.server_1:wait_until_ready()
    g.server_2:wait_until_ready()

    g.server_1:exec(function()
        t.assert_equals(#box.info.replication, 2)
        local peer
        if box.info.replication[2].name == 'instance-002' then
            peer = box.info.replication[2].upstream.peer
        else
            peer = box.info.replication[1].upstream.peer
        end
        local exp = 'replicator@unix/:./instance-002.iproto?transport=plain'
        t.assert_equals(peer, exp)
    end)
end

g.test_iproto_listen_ssl = function()
    t.tarantool.skip_if_enterprise()
    helpers.failure_case({
        options = {
            ['iproto.listen'] = {
                {
                    uri = 'unix/:./{{ instance_name }}.iproto',
                    params = {transport = 'ssl'},
                },
            },
        },
        exp_err = 'SSL is not available in this build',
    })
end

g.test_iproto_listen_ssl_enterprise = function()
    t.tarantool.skip_if_not_enterprise()
    local dir = treegen.prepare_directory({}, {})
    local passwd = '123qwe'
    local passwd_file = fio.pathjoin(dir, 'passwd.txt')
    local file = fio.open(passwd_file, {'O_WRONLY', 'O_CREAT'},
                          tonumber('666', 8))
    t.assert(file ~= nil)
    file:write(passwd)
    file:close()
    local cert_dir = fio.pathjoin(fio.abspath(os.getenv('SOURCEDIR') or '.'),
                                  'test/enterprise-luatest/ssl_cert')
    local ca_file = fio.pathjoin(cert_dir, 'ca.crt')
    local cert_file = fio.pathjoin(cert_dir, 'client.crt')
    local key_file = fio.pathjoin(cert_dir, 'client.enc.key')
    local ciphers = 'ECDHE-RSA-AES256-GCM-SHA384'

    local verify = function(ca, cert, key, password, password_file, cipher)
        local exp = {
            transport = 'ssl',
            ssl_ca_file = ca,
            ssl_cert_file = cert,
            ssl_key_file = key,
            ssl_password = password,
            ssl_password_file = password_file,
            ssl_ciphers = cipher,
        }
        t.assert_equals(#box.cfg.listen, 1)
        t.assert_equals(box.cfg.listen[1].uri, 'unix/:./instance-001.iproto')
        t.assert_equals(box.cfg.listen[1].params, exp)
    end

    helpers.success_case(g, {
        dir = dir,
        options = {
            ['iproto.listen'] = {{
                uri = 'unix/:./{{ instance_name }}.iproto',
                params = {
                    transport = 'ssl',
                    ssl_ca_file = ca_file,
                    ssl_cert_file = cert_file,
                    ssl_key_file = key_file,
                    ssl_password = passwd,
                    ssl_password_file = passwd_file,
                    ssl_ciphers = ciphers,
                },
            }},
        },
        verify = verify,
        verify_args = {ca_file, cert_file, key_file, passwd, passwd_file,
                       ciphers}
    })
end

g.test_replication_ssl_enterprise = function()
    t.tarantool.skip_if_not_enterprise()
    local dir = treegen.prepare_directory({}, {})
    local cert_dir = fio.pathjoin(fio.abspath(os.getenv('SOURCEDIR') or '.'),
                                  'test/enterprise-luatest/ssl_cert')

    local passwd_file = fio.pathjoin(dir, 'passwd.txt')
    local file = fio.open(passwd_file, {'O_WRONLY', 'O_CREAT'},
                          tonumber('666', 8))
    file:write('123qwe')
    t.assert(file ~= nil)
    local passwd = '1q2w3e'
    local ciphers = 'ECDHE-RSA-AES256-GCM-SHA384'

    -- Master certificates.
    local s1c = fio.pathjoin(cert_dir, 'server.crt')
    local s1k = fio.pathjoin(cert_dir, 'server.enc.key')
    local c1a = fio.pathjoin(cert_dir, 'ca.crt')
    local c1c = fio.pathjoin(cert_dir, 'client.crt')
    local c1k = fio.pathjoin(cert_dir, 'client.enc.key')
    -- Replica certificates.
    local s2c = fio.pathjoin(cert_dir, 'server2.crt')
    local s2k = fio.pathjoin(cert_dir, 'server2.key')
    local c2a = fio.pathjoin(cert_dir, 'ca2.crt')
    local c2c = fio.pathjoin(cert_dir, 'client2.crt')
    local c2k = fio.pathjoin(cert_dir, 'client2.key')

    local config = {
        credentials = {
            users = {
                guest = {
                    roles = {'super'},
                },
                replicator = {
                    password = 'topsecret',
                    roles = {'replication'},
                },
            },
        },
        groups = {
            ['group-001'] = {
                replicasets = {
                    ['replicaset-001'] = {
                        instances = {
                            ['instance-001'] = {
                                database = {
                                    mode = 'rw',
                                },
                                iproto = {
                                    listen = {{
                                        uri = 'unix/:./instance-001.iproto',
                                        params = {
                                            transport = 'ssl',
                                            ssl_ca_file = c1a,
                                            ssl_cert_file = s1c,
                                            ssl_key_file = s1k,
                                            ssl_password = passwd,
                                            ssl_ciphers = ciphers,
                                        },
                                    }},
                                    advertise = {
                                        peer = {
                                            login = 'replicator',
                                            uri = 'unix/:./instance-001.iproto',
                                            params = {
                                                transport = 'ssl',
                                                ssl_cert_file = c1c,
                                                ssl_key_file = c1k,
                                                ssl_password_file = passwd_file,
                                                ssl_ciphers = ciphers,
                                            },
                                        },
                                    },
                                },
                            },
                            ['instance-002'] = {
                                iproto = {
                                    listen = {{
                                        uri = 'unix/:./instance-002.iproto',
                                        params = {
                                            transport = 'ssl',
                                            ssl_ca_file = c2a,
                                            ssl_cert_file = s2c,
                                            ssl_key_file = s2k,
                                            ssl_ciphers = ciphers,
                                        },
                                    }},
                                    advertise = {
                                        peer = {
                                            login = 'replicator',
                                            uri = 'unix/:./instance-002.iproto',
                                            params = {
                                                transport = 'ssl',
                                                ssl_cert_file = c2c,
                                                ssl_key_file = c2k,
                                                ssl_ciphers = ciphers,
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }
    local config_file = treegen.write_file(dir, 'config.yaml',
                                           yaml.encode(config))
    g.server_1 = server:new({
        config_file = config_file,
        chdir = dir,
        alias = 'instance-001',
    })
    g.server_2 = server:new({
        config_file = config_file,
        chdir = dir,
        alias = 'instance-002',
    })

    g.server_1:start({wait_until_ready = false})
    g.server_2:start({wait_until_ready = false})

    g.server_1:wait_until_ready()
    g.server_2:wait_until_ready()

    g.server_1:exec(function(cert_file, key_file, ciphers)
        t.assert_equals(#box.info.replication, 2)
        local res = require('uri').parse(box.info.replication[2].upstream.peer)
        t.assert_equals(res.params.ssl_cert_file, {cert_file})
        t.assert_equals(res.params.ssl_key_file, {key_file})
        t.assert_equals(res.params.ssl_ciphers, {ciphers})
    end, {c2c, c2k, ciphers})

    g.server_2:exec(function(cert_file, key_file, password_file, ciphers)
        t.assert_equals(#box.info.replication, 2)
        local res = require('uri').parse(box.info.replication[1].upstream.peer)
        t.assert_equals(res.params.ssl_cert_file, {cert_file})
        t.assert_equals(res.params.ssl_key_file, {key_file})
        t.assert_equals(res.params.ssl_ciphers, {ciphers})
        t.assert_equals(res.params.ssl_password_file, {password_file})
    end, {c1c, c1k, passwd_file, ciphers})
end

-- Verify that all the non-dynamic options that are changed on the
-- configuration reloading are reported in alerts.
g.test_non_dynamic_alert = function(g)
    helpers.reload_success_case(g, {
        options = {},
        verify = function() end,
        options_2 = {
            ['process.strip_core'] = false,
            ['process.background'] = true,
            ['process.work_dir'] = 'foo',
        },
        verify_2 = function()
            local config = require('config')

            local options = {}
            local info = config:info()
            for _, alert in ipairs(info.alerts) do
                if alert.message:find('non%-dynamic option') then
                    local pattern = 'non%-dynamic option (.-) will not be set'
                    local option = alert.message:match(pattern)
                    table.insert(options, option)
                end
            end
            table.sort(options)

            t.assert_equals({
                status = info.status,
                options = options,
            }, {
                status = 'check_warnings',
                options = {
                    'background',
                    'strip_core',
                    'work_dir',
                },
            })
        end,
    })
end

-- Verify that status now is changeg only after post apply.
g.test_post_apply_status = function(cg)
    local one = string.dump(function()
        local function apply()
            rawset(_G, 'status_roles', require('config'):info().status)
        end

        return {
            validate = function() end,
            apply = apply,
            stop = function() end,
        }
    end)

    local script = string.dump(function()
        rawset(_G, 'status_app', require('config'):info().status)
    end)

    local function verify()
        t.assert_equals(rawget(_G, 'status_app'), 'startup_in_progress')
        t.assert_equals(rawget(_G, 'status_roles'), 'startup_in_progress')
    end

    local function verify_2()
        t.assert_equals(rawget(_G, 'status_app'), 'reload_in_progress')
        t.assert_equals(rawget(_G, 'status_roles'), 'reload_in_progress')
    end

    helpers.reload_success_case(cg, {
        roles = {one = one},
        script = script,
        options = {
            ['roles'] = {'one'},
            ['app.file'] = 'main.lua',
        },
        verify = verify,
        verify_2 = verify_2,
    })
end

-- Check that config:info('v2') works properly.
g.test_info_new_version = function(g)
    local dir = treegen.prepare_directory({}, {})

    local config = [[
        credentials:
          users:
            guest:
              roles:
              - super

        iproto:
          listen:
            - uri: unix/:./{{ instance_name }}.iproto

        groups:
          group-001:
            replicasets:
              replicaset-001:
                instances:
                  instance-001: {}
    ]]
    treegen.write_file(dir, 'config.yaml', config)

    -- New source that sets meta.
    local script = string.dump(function()
        local yaml = require('yaml')
        local file = require('internal.config.utils.file')

        local methods = {}
        local mt = {
            __index = methods,
        }

        function methods.sync(self, config)
            local data = file.universal_read('myconf.yaml', 'my config file')
            local ok, res = pcall(yaml.decode, data)
            if not ok then
                error('Unable to parse my config file')
            end
            self._values = res
            config:_meta('mysource', 'revision', self._revision)
            self._revision = self._revision + 1
        end

        function methods.get(self)
            return self._values
        end

        local function new()
            return setmetatable({
                name = 'mysource',
                type = 'instance',
                _values = {},
                _revision = 1,
            }, mt)
        end

        return {
            new = new,
        }
    end)
    treegen.write_file(dir, 'mysource.lua', script)

    local myconf = [[
        sql:
          cache_size: 12345
    ]]
    treegen.write_file(dir, 'myconf.yaml', myconf)

    local opts = {
        config_file = 'config.yaml',
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()

    -- Make sure the new source is loaded properly and the meta is set.
    g.server:exec(function()
        local config = require('config')
        t.assert_equals(config:get('sql.cache_size'), 5242880)

        config:_register_source(require('mysource').new())
        config:reload()
        t.assert_equals(config:get('sql.cache_size'), 12345)
        local exp = {
            mysource = {
                revision = 1,
            },
        }
        t.assert_equals(config:info().meta, exp)
        t.assert_equals(config:info('v1').meta, exp)
        exp = {
            active = {
                mysource = {
                    revision = 1,
                },
            },
            last = {
                mysource = {
                    revision = 1,
                },
            },
        }
        t.assert_equals(config:info('v2').meta, exp)
    end)

    -- Make sure the meta for config:info() is new and that config:info('v2')
    -- contains both applied meta and unapplied new meta when reload failed
    -- in apply phase.
    myconf = [[
        sql:
          cache_size: true
    ]]
    treegen.write_file(dir, 'myconf.yaml', myconf)
    g.server:exec(function()
        local config = require('config')
        t.assert_equals(config:get('sql.cache_size'), 12345)
        pcall(config.reload, config)
        t.assert_equals(config:get('sql.cache_size'), 12345)
        local exp = {
            mysource = {
                revision = 2,
            },
        }
        t.assert_equals(config:info().meta, exp)
        t.assert_equals(config:info('v1').meta, exp)
        exp = {
            active = {
                mysource = {
                    revision = 1,
                },
            },
            last = {
                mysource = {
                    revision = 2,
                },
            },
        }
        t.assert_equals(config:info('v2').meta, exp)
    end)

    -- Make sure the meta for config:info() is new and that config:info('v2')
    -- contains both applied meta and unapplied new meta when reload failed
    -- in post_apply phase.
    myconf = [[
        sql:
          cache_size: 123
        roles: [myrole]
    ]]
    treegen.write_file(dir, 'myconf.yaml', myconf)
    g.server:exec(function()
        local config = require('config')
        t.assert_equals(config:get('sql.cache_size'), 12345)
        t.assert_equals(config:get('roles'), nil)
        local ok, err = pcall(config.reload, config)
        t.assert(not ok)
        t.assert_str_contains(err, "module 'myrole' not found")
        -- If error in post_apply phase get() returns config of failed load.
        t.assert_equals(config:get('sql.cache_size'), 123)
        t.assert_equals(config:get('roles'), {'myrole'})
        local exp = {
            mysource = {
                revision = 3,
            },
        }
        t.assert_equals(config:info().meta, exp)
        t.assert_equals(config:info('v1').meta, exp)
        exp = {
            active = {
                mysource = {
                    revision = 1,
                },
            },
            last = {
                mysource = {
                    revision = 3,
                },
            },
        }
        t.assert_equals(config:info('v2').meta, exp)
    end)

    -- Make sure that in config:info('v2') both applied meta and unapplied new
    -- meta are equal when reload is successful.
    myconf = [[
        sql:
          cache_size: 100000
    ]]
    treegen.write_file(dir, 'myconf.yaml', myconf)
    g.server:exec(function()
        local config = require('config')
        t.assert_equals(config:get('sql.cache_size'), 123)
        t.assert_equals(config:get('roles'), {'myrole'})
        config:reload()
        t.assert_equals(config:get('sql.cache_size'), 100000)
        t.assert_equals(config:get('roles'), nil)
        local exp = {
            mysource = {
                revision = 4,
            },
        }
        t.assert_equals(config:info().meta, exp)
        t.assert_equals(config:info('v1').meta, exp)
        exp = {
            active = {
                mysource = {
                    revision = 4,
                },
            },
            last = {
                mysource = {
                    revision = 4,
                },
            },
        }
        t.assert_equals(config:info('v2').meta, exp)
    end)
end

-- Check that config:info() throws an error on unexpected argument.
g.test_info_version_error = function(g)
    local verify = function()
        local config = require('config')
        local exp = 'config:info() expects v1, v2 or nil as an argument, ' ..
            'got "1"'
        t.assert_error_msg_equals(exp, config.info, config, 1)
    end

    helpers.success_case(g, {
        verify = verify,
    })
end

-- Ensure that the configuration module does not revoke privilege credentials
-- that it has not granted.
g.test_do_no_revoke_user_privs = function(g)
    local dir = treegen.prepare_directory({}, {})
    local config = {
        credentials = {
            users = {
                guest = {
                    roles = {'super'},
                },
            },
        },
        iproto = {
            listen = {{
                uri = 'unix/:./{{ instance_name }}.iproto',
            }},
        },
        groups = {
            ['group-001'] = {
                replicasets = {
                    ['replicaset-001'] = {
                        instances = {
                            ['instance-001'] = {},
                        },
                    },
                },
            },
        },
    }
    treegen.write_file(dir, 'config.yaml', yaml.encode(config))
    local opts = {
        config_file = 'config.yaml',
        alias = 'instance-001',
        chdir = dir,
    }
    g.server = server:new(opts)
    g.server:start()

    -- Create a space for which privileges will be checked.
    g.server:exec(function()
        box.schema.space.create('a')
    end)

    -- Create a role and grant it some privileges using config and
    -- box.schema.user.grant().
    config.credentials.roles = {myrole = {
            privileges = {{
                permissions = {'read', 'write'},
                spaces = {'a'},
            }},
        },
    }
    treegen.write_file(dir, 'config.yaml', yaml.encode(config))
    g.server:exec(function()
        local config = require('config')
        config:reload()
        local config_privs = config:get('credentials.roles.myrole')
        local exp = {
            privileges = {{
                permissions = {"read", "write"},
                spaces = {"a"},
            }},
        }
        t.assert_equals(config_privs, exp)
        local actual_privs = box.schema.role.info('myrole')
        exp = {{"read,write", "space", "a"}}
        t.assert_equals(actual_privs, exp)

        box.schema.role.grant('myrole', 'read,create', 'space', 'a')
        actual_privs = box.schema.role.info('myrole')
        exp = {{"read,write,create", "space", "a"}}
        t.assert_equals(actual_privs, exp)
    end)

    -- Change the privileges granted by config. Ensure that privileges
    -- that were not granted by the configuration, as well as
    -- privileges that were not granted solely by the configuration,
    -- are not revoked on reload.
    config.credentials.roles = {myrole = {
            privileges = {{
                permissions = {'drop', 'read'},
                spaces = {'a'},
            }},
        },
    }
    treegen.write_file(dir, 'config.yaml', yaml.encode(config))
    g.server:exec(function()
        local config = require('config')
        config:reload()
        local config_privs = config:get('credentials.roles.myrole')
        local exp = {
            privileges = {{
                permissions = {"drop", "read"},
                spaces = {"a"},
            }},
        }
        t.assert_equals(config_privs, exp)

        local actual_privs = box.schema.role.info('myrole')
        exp = {{"read,create,drop", "space", "a"}}
        t.assert_equals(actual_privs, exp)
    end)
end

-- This scenario has been broken before gh-10855.
-- Before gh-10855 config:get() used to change the passed path
-- if it's been passed as a table.
g.test_get_not_changes_path = function(g)
    local verify = function()
        local config = require('config')
        local path = {'fiber', 'slice'}

        -- The result of config:get() isn't tested.
        local _ = config:get(path)
        -- Make sure the passed path hasn't changed.
        t.assert_equals(path, {'fiber', 'slice'})

    end
    helpers.success_case(g, {
        verify = verify,
    })
end
