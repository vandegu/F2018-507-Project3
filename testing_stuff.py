a = [('hello',45.4,15),('hello',45.4,15),('bye',25.4,13)]

pout = ''
for r in a:
    for i in r:
        print(type(i))
        if type(i) == '<class \'float\'>':
            pout += '{:.>20f}'.format(i)
        else:
            pout += '{:.<20s}'.format(str(i))
    pout += '\n'

print(pout)
