from datetime import datetime, timedelta, tzinfo

class UTC(tzinfo):

    ZERO = timedelta(0)

    def utcoffset(self, dt):
        return self.ZERO
        
    def tzname(self):
        return "UTC"
        
    def dst(self, dt):
        return self.ZERO

class ShiftTZ(tzinfo):
    
    def __init__(self, shift):
        self.Shift = shift
        
    def utcoffset(self, dt):
        return timedelta(hours=self.Shift)
        
    def tzname(self):
        return '%+02d:00' % (self.Shift,)
        
    def dst(self, dt):
        return self.ZERO

def epoch(t):
    if t is None:   return None
    if isinstance(t, (int, float)):    return t
    delta = t - datetime(1970,1,1,tzinfo=UTC())
    return delta.days * 3600 * 24 + delta.seconds + float(delta.microseconds)/1000000.0;

def text2datetime(t):
    if t == None:
        t = datetime.now()
    else:
        try:
            t = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%f')
            t = t.replace(tzinfo=UTC())
        except:
            try:    
                t = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
                t = t.replace(tzinfo=UTC())
            except:
                #print sys.exc_type, sys.exc_value
                try:    
                    t = datetime.strptime(t, '%m/%d/%Y %H:%M:%S')
                    t = t.replace(tzinfo=UTC())
                except:
                    try:
                        d,t = tuple(t.split(None, 1))
                        tz = None
                        if '-' in t:  
                            t,tz = tuple(t.split('-'))
                            tz = ShiftTZ(-int(tz))
                        elif '+' in t: 
                            t,tz = tuple(t.split('+'))
                            tz = ShiftTZ(int(tz))
                        #print 'tz=', tz
                        t = datetime.strptime('%sT%s' % (d,t), 
                                    '%Y-%m-%dT%H:%M:%S')
                        if tz:
                            t = t.replace(tzinfo=tz)
                    except:
                        pass
    if isinstance(t, (str, unicode)):
        unit = 's'
        if t[-1] in 'dhms':
            unit = t[-1]
            t = t[:-1]
        t = float(t)
        if t < 0:
            mult = {
                'd':    24*3600,
                'h':    3600,
                'm':    60,
                's':    1
                }[unit]
            t = t * mult
    if isinstance(t, (int, float)):
        if t < 0:
            t = datetime.utcnow() - timedelta(seconds=-t)
            t = t.replace(tzinfo=UTC())
        else:
            t = datetime.utcfromtimestamp(t)
            t = t.replace(tzinfo=UTC())
    #print type(t), t
    return t
