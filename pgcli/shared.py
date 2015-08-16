# ---- Module for shared data that should persist when other modules that load it
# are reloaded. It is designed to allow faster development of python code.

entered_code = ''           # the command that was entered, ignoring meta options
executed_sql = ''           # the actual SQL that is executed

macros = {}                 # macro formulas
info = {}                   # env for eval; this is passed along when a macro invokes another macro

headers = []
table = []                  # last table info

# useful debugging values
_it_var_name =''            # variable to save results into
_it = {}                    # saved by variable name
_it_entered_code = {}
_it_executed_sql = {}

# Customized coloring of sql results.
# Format is: table_column1:color_spec1~color_spec2~...~color_specN//table_column2:color_spec1~color_spec2~...~color_specN
# For each table column, values can be colored differently. The color progression is:
#        ['red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'white']
# Each spec can specify an arbitrary number of values to render in that color, separated by "|", e.g.
#       car_type:van|minivan|truck~convertible|sport~truck//num_doors:2~4
# For the "car_type" column:
#    "van", "minivam", or "truck" is colored "red"
#    "convertible" or "sport" is colored "green"
#    "truck" is colored "yellow"
# For the "num_doors" column the color progression starts all over again:
#    "2" is colored "red"
#    "4" is colored "green"


local_color_specs = []
local_color_specs_add_to_default = True

# a "+" in front of the name means that it should be part of default
color_specs = {
    #'+accounting' : 'name:cash on hand|Office supplies~VAT input~Custom tax//ucode:~~551450',
    'car_colorings' : "table_column1:color_spec1~color_spec2~...~color_specN//table_column2:color_spec1~color_spec2~...~color_specN",
    '+accounting_colorings' : 'name:cash on hand~account payable//account_ccode:1100|1101~2000|2001|2002'
}

# For greater customization, for each column name, you can specify the texts of interest using a predicate
default_color_specs = [
    #('name', 'cash on hand'),
    #('name', lambda x: x.find('Office supplies')>=0),
    #('amount', lambda x: x < 600000),
    #('date', lambda d: str(d) > '2009-11-03')
]

