#!/usr/bin/env python

import pymysql


def main(hosts: list) -> list:
    results = {}

    for host in hosts:
        host_result = {'host': host['host']}

        # we're going to use **kwargs representation of a dictionary here so we don't have to declare each variable
        # this means the values will be passed exactly as we provide them in the hosts.yaml file
        with pymysql.connect(**host) as mysql:  # with statements automatically close database connections when done
            db_cursor = mysql.cursor(pymysql.cursors.DictCursor)    # we'll use a DictCursor here to return the results
                                                                    # as a dictionary

            # let's gather everything: schemas, tables, and columns at once; we'll do comparison **last**
            db_cursor.execute('select table_schema, table_name, column_name from columns')
            result_set = db_cursor.fetchall() or []     # the 'or []' means we'll get an empty list if the result_set is
                                                        # None; this is useful to prevent future errors, like those in
                                                        # the following list comprehension

        # notice that we went back an indent here: the mysql connection should now be closed

        """
        Now we must convert the response into something useful. A nested list or dictionary object would be a suboptimal
        approach because we need to compare these objects. If we kept them nested, we would have to walk the tree to
        to do any comparisons.
        
        This would suck.
        
        So, we're going to use a technique called "flattening" whereby we take nested objects and create a single list
        consisting of all nested data points. Typically, I'd use the python flatten_json package, but it won't work here
        because of the way the data is currently architected. Instead, we can just write a quick list comprehension to
        take many fiends and convert them into individual fields. Without nesting, the "ideal" structure would probably
        be:
        
        d = {"schema_a": {"table_a": ["column_a", "column_b"],
                          "table_b": ["column_c", "column_d"]},
             
             "schema_b": {"table_a": ["column_a", "column_b"],
                          "table_b": ["column_c", "column_d"]}}

        With flattened, we'll get:
            d = ["schema_a.table_a.column_a",
             "schema_a.table_a.column_b",
             "schema_a.table_b.column_c",
             "schema_a.table_b.column_d",
             "schema_b.table_a.column_a",
             "schema_b.table_a.column_b",
             "schema_b.table_b.column_c",
             "schema_b.table_b.column_d"]
        
        We could use any separator, such as #, as long as it was logical but since column names can contain almost 
        anything and decimal-delimited is also acceptable syntax for mysql, it makes the most sense to attempt that here        
        """

        results[host.host] = ['.'.join([row['table_schema'],
                                        row['table_name',
                                        row['column_name']]]) for row in result_set]

    # here I want to know what the host we'll be comparing everything to is
    base_host = results[hosts[0]]
    base_host_name = hosts[0].host

    # the only way to know if something is in every host is to actually list them all
    # this nested list comprehension will loop over each row in each host and put them in one master list
    # from there, we can check if each object exists in each row
    all_schema_objects = [row for row in [host for host in hosts]]
    all_schema_objects.sort()

    # now we'll loop over the objects in the master list
    report = []
    for schema_object in all_schema_objects:
        # we'll create a new row dict for each object we have. The name will be given the 'object_name' key
        # since we already know who the base host is, we'll also perform a local check to see if the schema object we're
        # currently looking at is in the base host
        row = {'object_name': schema_object,
               base_host_name: schema_object in base_host}

        # now we'll loop over all the other hosts you might compare. Since the program is designed to allow more than
        # two hosts, you can supply as many as python can hold
        # the loop here will go over every object *except* the base_host which we'll ignore because we already did that
        # one above.
        for host in [k for k in results.keys() if k != base_host]:
            row[host] = schema_object in host

    # once all of the values are complete, return the report
    return report


if __name__ == 'main':
    # setup command line arguments
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('hosts', type=str, help='a yaml file containing the target hosts and '
                                                'their respective credentials')
    args = parser.parse_args()

    # check that that host file is present
    import os
    if not os.path.exists(args.hosts):
        print(f'{args.hosts} is not a valid file name')
        exit(1)

    # try to load the yaml file itself
    try:
        import yaml
        with open(args.hosts, 'r') as yaml_file:
            host_file = yaml.load(yaml_file, Loader=yaml.SafeLoader)

    except yaml.YAMLError as ye:
        print(f'could not load error: {" ".join(ye.args)}')

    # this is a call to the main body of the work
    result = main(hosts=args.hosts)

    # tabulate will convert the main() output into something human-readable
    from tabulate import tabulate
    print(tabulate(tabular_data=result, headers='keys'))
