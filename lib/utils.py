import csv
import json

def lcy_flatten_json(data, delim="__"):
    """
        Convert tree-like json/dict object into single-level dict object.

        Args:
            data:  a single dict/json object.
            delim: delimiter to paste keys in different-levels. Default is two underscores ("__" )

        Returns:
            A dict/json object
    """
    val = {}
    for i in data.keys():
        if isinstance( data[i], dict ):
            get = lcy_flatten_json( data[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = data[i]
    return val

def lcy_flatten_jsonlist(data, delim="__"):
    """
        Convert a list of tree-like json/dict objects into a list of single-level dict objects.

        Args:
            data:  a list of dict/json objects.
            delim: delimiter to paste keys in different-levels. Default is two underscores ("__" )

        Returns:
            A list of single-level dict/json objects
    """
    return map( lambda x: lcy_flatten_json(data=x, delim=delim), data )


def lcy_read_jsonlist(file):
    """
        Read json-list file. Each line is a json.

        Args:
            file:  a single dict/json object.

        Returns:
            A list of json
    """
    data = []
    with open(file, "r") as f:
        for jsonline in f:
            data.append(json.loads(jsonline))
    return data

def lcy_write_jsonlist2csv(data, file, mode='wb', consistent=True):
    """
        write a list of json object to a file. Each dict object can have different key set.

        Args:
            data:   a list of json object
            file:   a file name.
            mode:   mode of open file (default 'wb')
            consistent: bool value; default is true, which means dict objects in the list have exactly the same key set.

        Returns:
            None
    """
    if consistent:
        keys = data[0].keys()
        with open(file, mode) as f:
            dict_writer = csv.DictWriter(f, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
    else:
        columns = map( lambda x: x.keys(), data )
        columns = reduce( lambda x,y: x+y, columns )
        columns = list( set( columns ) )
        with open( file, 'wb' ) as f:
            csv_w = csv.writer( f )
            csv_w.writerow( columns )
            for i_r in data:
                csv_w.writerow( map( lambda x: i_r.get( x, "" ), columns ) )

def lcy_jsonlist2csv(data, file, consistent=False):
    """
        Convert a list of json/dict objects into a list of single-level dict objects and write to a file in a csv format.

        Args:
            data:   a list of json object
            file:   a file to write.
            consistent: bool value; default is true, which means dict objects in the list have exactly the same key set.

        Returns:
            None
    """
    output = lcy_flatten_jsonlist(data)
    lcy_write_jsonlist2csv(data=output, file=file, consistent=consistent)

if __name__ == "__main__":
    data = lcy_read_jsonlist(file='/home/eijmmmp/bdcv1_mrsv1_201601014.json')
    lcy_jsonlist2csv(data=data, file='/home/eijmmmp/bdcv1_mrsv1_201601014.csv')
