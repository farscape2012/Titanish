def str_swap(string, i, j):
    """ String swap given two postions """
    if i > len(string) or j > len(string):
        return string
    lst = list(string)
    lst[i], lst[j] = lst[j], lst[i]
    return ''.join(lst)
