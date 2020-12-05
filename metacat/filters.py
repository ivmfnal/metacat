#
# Common filters
#

def sample(inputs, params, limit=None, **ignore):
    file_set = inputs[0]
    fraction = params[0]
    x = 0.0
    for f in file_set:
        x += fraction
        if x >= 1.0:
            x -= 1.0
            yield f
            if limit is not None:
                limit -= 1
                if limit <= 0:
                    break
            
def limit(inputs, params, **ignore):
    file_set = inputs[0]
    n = params[0]
    for f in file_set:
        if n <= 0:
            break
        yield f
        n -= 1

        
standard_filters = dict(sample=sample, limit=limit)
            
