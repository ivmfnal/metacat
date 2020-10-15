#
# Common filters
#

def sample(inputs, params):
    file_set = inputs[0]
    fraction = params[0]
    x = 0.0
    for f in file_set:
        x += fraction
        if x >= 1.0:
            x -= 1.0
            yield f
            
def limit(inputs, params):
    file_set = inputs[0]
    n = params[0]
    for f in file_set:
        if n <= 0:
            break
        yield f
        n -= 1

        
filters_map = dict(sample=sample, limit=limit)
            
