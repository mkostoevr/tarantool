--
-- Usage: <script> file1 file2 ...
--
-- An input file is supposed to contain test results in the following format:
--
-- <test1> <result1> rps
-- <test2> <result1> rps
-- ...
--
-- Here <testN> is the name of a test case (string, no spaces) and <resultN>
-- is the test case result (number).
--
-- The script reads the result files and outputs the results in a convenient
-- for comparison form:
--
--                  <file1>                  <file2>
-- <test1>  <file1-result1>  <file2-result2> (+NNN%)
-- <test2>  <file1-result2>  <file2-result2> (+NNN%)
-- ...
--

-- Perform the Student's t-test to check if the changes are significant.
function t_test(a_count, b_count, a_avg, b_avg, a_var, b_var, p)
    assert(type(a_count) == 'number')
    assert(type(b_count) == 'number')
    assert(type(a_avg) == 'number')
    assert(type(b_avg) == 'number')
    assert(type(a_var) == 'number')
    assert(type(b_var) == 'number')
    assert(type(p) == 'number' or p == nil)

    p = p or 0.002

    -- Sources of values:
    -- https://en.wikipedia.org/wiki/Student%27s_t-distribution
    -- https://www.itl.nist.gov/div898/handbook/eda/section3/eda3672.htm
    local distribution = {}
    distribution[0.002] = {
        -- df = 1..10.
        318.31, 22.327, 10.215, 7.173, 5.893, 5.208, 4.785, 4.501, 4.297, 4.144,
        -- df = 11..20.
        4.025, 3.930, 3.852, 3.787, 3.733, 3.686, 3.646, 3.610, 3.579, 3.552,
        -- df = 21..30.
        3.527, 3.505, 3.485, 3.467, 3.450, 3.435, 3.421, 3.408, 3.396, 3.385,
        -- df = 31..40.
        3.375, 3.365, 3.356, 3.348, 3.340, 3.333, 3.326, 3.319, 3.313, 3.307,
        -- df = 41..50.
        3.301, 3.296, 3.291, 3.286, 3.281, 3.277, 3.273, 3.269, 3.265, 3.261,
        -- df = 51..60.
        3.258, 3.255, 3.251, 3.248, 3.245, 3.242, 3.239, 3.237, 3.234, 3.232,
        -- df = 61..70.
        3.229, 3.227, 3.225, 3.223, 3.220, 3.218, 3.216, 3.214, 3.213, 3.211,
        -- df = 71..80.
        3.209, 3.207, 3.206, 3.204, 3.202, 3.201, 3.199, 3.198, 3.197, 3.195,
        -- df = 81..90.
        3.194, 3.193, 3.191, 3.190, 3.189, 3.188, 3.187, 3.185, 3.184, 3.183,
        -- df = 91..100.
        3.182, 3.181, 3.180, 3.179, 3.178, 3.177, 3.176, 3.175, 3.175, 3.174,
        -- df = ∞.
        3.090,
    }

    local volatility_coefficient = 0.25 -- Tuning for the execution environment.

    local mean_difference = math.abs(a_avg - b_avg)
    local standard_error = math.sqrt(a_var / a_count + b_var / b_count)
    print(a_count, b_count, a_avg, b_avg, a_var, b_var)
    print(mean_difference)
    print(a_var / a_count + b_var / b_count)
    print(standard_error)
    local t_value = (mean_difference / standard_error) * volatility_coefficient

    assert(a_count > 1 and b_count > 1)
    -- Floor with 0.5 added is the lua way to round up, df needs to be integer.
    local df = math.floor(math.pow(a_var / a_count + b_var / b_count, 2) /
                          ((1 / (a_count - 1)) * math.pow(a_var / a_count, 2) +
                           (1 / (b_count - 1)) * math.pow(b_var / b_count, 2)) +
                          0.5)

    local t_value_critical = distribution[p][math.min(df, #distribution[p])]

    print(t_value, t_value_critical)

    if t_value > t_value_critical then
        if a_avg > b_avg then
            result = t_value > t_value_critical * 1.5 and 'Regression'
                                                       or 'Suspecious'
        else
            result = t_value > t_value_critical * 1.5 and 'Improvement'
                                                       or 'Uncertain'
        end
        return string.format("%s (t-value %.2f > %.2f)", result, t_value, t_value_critical)
    else
        return "~"
    end
end

-- Set of test names: <test-name> -> true.
local test_names = {}

-- Test names in the order of appearance.
local test_names_ordered = {}

-- Array of result columns.
local columns = {}

-- Width reserved for the first column (the one with the test names).
local column0_width = 0

-- Read the results from the input files.
for i, file_name in ipairs(arg) do
    local column = {
        name = file_name,
        values = {},
    }
    local width_extra = 2 -- space between columns
    if i > 1 then
        -- Reserve space for diff percentage: ' (+NNN%)'
        width_extra = width_extra + 8
    end
    column.width = string.len(column.name) + width_extra
    table.insert(columns, column)
    for line in io.lines(file_name) do
        -- <name> <number> rps
        local test_name, value, _ = unpack(string.split(line))
        if not test_names[test_name] then
            table.insert(test_names_ordered, test_name)
            test_names[test_name] = true
        end
        column0_width = math.max(column0_width, string.len(test_name))
        if not column.values[test_name] then
            column.values[test_name] = {}
        end
        table.insert(column.values[test_name], tonumber(value))
        column.width = math.max(column.width, string.len(value) + width_extra)
    end
end

-- Now columns contain tests with results, need to transform them into stats.
for _, column in pairs(columns) do
    for test_name, values in pairs(column.values) do
        -- Calculate the mean.
        local count = #values
        local sum = 0
        for _, value in ipairs(values) do
            sum = sum + value
        end
        values.avg = sum / count

        -- Calculate the variance.
        local mean_diff_square_sum = 0
        for _, value in ipairs(values) do
            mean_diff_square_sum = mean_diff_square_sum +
                                   math.pow(value - values.avg, 2)
        end
        values.var = mean_diff_square_sum / count

        -- Stringify it.
        local width_extra = 2 -- Space between columns.
        values.str = string.format('%.02f', values.avg)
        column.width =
            math.max(column.width, string.len(values.str) + width_extra)
    end
end

-- Print the header row.
local line = string.rjust('', column0_width)
for _, column in ipairs(columns) do
    line = line .. string.rjust(column.name, column.width)
end
print(line)

-- Print the result rows.
for _, test_name in ipairs(test_names_ordered) do
    local line = string.rjust(test_name, column0_width)
    for i, column in ipairs(columns) do
        local value = column.values[test_name].avg
        if not value then
            value = 'NA'
        end
        if i > 1 then
            local t_test_result = t_test(#columns[1].values[test_name],
                                         #columns[i].values[test_name],
                                         columns[1].values[test_name].avg,
                                         columns[i].values[test_name].avg,
                                         columns[1].values[test_name].var,
                                         columns[i].values[test_name].var)
            local diff = ' (+ NA%)'
            local curr = value
            local base = columns[1].values[test_name].avg
            if base and curr then
                diff = 100 * (curr - base) / base
                diff = string.format(' (%s%3d%%, %s)', diff >= 0 and '+' or '-',
                                     math.abs(diff), t_test_result)

            end
            value = column.values[test_name].str .. diff
        else
            value = column.values[test_name].str
        end
        line = line .. string.rjust(value, column.width)
    end
    print(line)
end
