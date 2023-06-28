import re

def validate_metadata(definitions, restricted, metadata={}, name=None, value=None):
    """
    Validates metadata against a set of definitions. Can be used to validate category and dataset restrictions
    
    Parameters
    ----------
    metadata : dict
        metadata dictionary to validate
    definitions : dict
        metadata definitions, indexed by the parameter name:
            {
                "parameter_name" {
                    ... definition ...
                }
            }
        
        definition may include:
            type : string, one of: int, float, text, boolean, dict, list, int[], float[], text[], boolean[], any
            min : minimal acceptable value (for numeric and strings)
            max : max acceptable value (for numeric and strings)
            values : list, enumeration of values
            pattern : regexp pattern for text strings
            required : boolean, whether the parameter is required
    restricted : boolean
        whether the validation must be done in "restricted" mode - if a parameter is not mentioned in the definitions, flag that as an error

    Returns
    -------
    list
        list of tuples (name, error) - error mesages for parameters
        empty list indicates that validation succeeded
    """

    definitions = definitions or {}
    
    metadata = (metadata or {}).copy()
    if name is not None:
        metadata[name] = value

    errors = []
    for name, value in metadata.items():
        
        definition = definitions.get(name)
        if definition is None:    
            if restricted:
                errors.append((name, "parameter not allowed in restricted category"))
            continue

        typ = definition.get("type")
        type_mismatch = False
        repv = repr(value)
        if typ is not None:
            if typ == "any":
                continue
        
        
            if typ == "int" and not isinstance(value, int): 
                errors.append((name, f"scalar int value required instead of {repv}"))
                type_mismatch = True
            elif typ == "float" and not isinstance(value, float): 
                errors.append((name, f"scalar float value required instead of {repv}"))
                type_mismatch = True
            elif typ == "text" and not isinstance(value, str): 
                errors.append((name, f"scalar text value required instead of {repv}"))
                type_mismatch = True
            elif typ == "boolean" and not isinstance(value, bool): 
                errors.append((name, f"scalar boolean value required instead of {repv}"))
                type_mismatch = True
            elif typ == "dict" and not isinstance(value, dict): 
                errors.append((name, f"dict value required instead of {repv}"))
                type_mismatch = True
            elif typ == "list" and not isinstance(value, list): 
                errors.append((name, f"list value required instead of {repv}"))
                type_mismatch = True

            elif typ == "int[]":
                if not isinstance(value, list): 
                    errors.append((name, f"list of ints required instead of {repv}"))
                    type_mismatch = True
                if not all(isinstance(x, int) for x in value): 
                    errors.append((name, f"list of ints required instead of {repv}"))
                    type_mismatch = True

            elif typ == "float[]":
                if not isinstance(value, list): 
                    errors.append((name, f"list of floats required"))
                    type_mismatch = True
                if not all(isinstance(x, float) for x in value): 
                    errors.append((name, f"list of floats required instead of {repv}"))
                    type_mismatch = True
            
            elif typ == "text[]":
                if not isinstance(value, list): 
                    errors.append((name, f"list of strings required"))
                    type_mismatch = True
                if not all(isinstance(x, str) for x in value): 
                    errors.append((name, f"list of strings required instead of {repv}"))
                    type_mismatch = True
            
            elif typ == "boolean[]":
                if not isinstance(value, list): 
                    errors.append((name, f"list of booleans required"))
                    type_mismatch = True
                if not all(isinstance(x, bool) for x in value): 
                    errors.append((name, f"list of booleans required instead of {repv}"))
                    type_mismatch = True
        
        if not type_mismatch:
            if not typ in ("boolean", "boolean[]", "list", "dict", "any"):
                if "values" in definition:
                    values = definition["values"]
                    if isinstance(value, list):
                        if not all(x in values for x in value): errors.append((name, f"value in {value} is not allowed"))
                    else:
                        if not value in values: errors.append((name, f"value {repv} is not allowed"))
                else:
                    if "pattern" in definition and typ in ("text", "text[]"):
                        pattern = definition["pattern"]
                        r = re.compile(pattern)
                        if isinstance(value, list):
                            if not all(r.match(v) is not None for v in value):  
                                errors.append((name, f"value in {value} does not match the pattern '{pattern}'"))
                        else:
                            if r.match(value) is None:
                                errors.append((name, f"value {value} does not match the pattern '{pattern}'"))
                    if "min" in definition:
                        vmin = definition["min"]
                        if isinstance(value, list):
                            if not all(x >= vmin for x in value):   
                                errors.append((name, f"value in {value} out of range (min:{vmin})"))
                        else:
                            if value < vmin:    
                                errors.append((name, f"value {value} out of range (min:{vmin})"))
                    if "max" in definition:
                        vmax = definition["max"]
                        if isinstance(value, list):
                            if not all(x <= vmax for x in value):   errors.append((name, f"value in {value} out of range (max:{vmax})"))
                        else:
                            if value > vmax:    errors.append((name, f"value {value} out of range (max:{vmax})"))

    for dname, definition in definitions.items():
        if definition.get("required") and dname not in metadata:
            errors.append((dname, "required parameter is missing"))
    
    return errors