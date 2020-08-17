#!/usr/bin/env python
import argparse
import math

def linspace(a, b, n):
    '''Numpyless linear spacing function.
    '''
    if n < 2:
        return a
    diff = (float(b) - a)/(n - 1)
    return [diff * i + a for i in range(n)]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('amp0', type=float)
    parser.add_argument('amp1', type=float)
    parser.add_argument('angle', type=float)
    parser.add_argument('number', type=int)

    args = parser.parse_args()
    amps = linspace(args.amp0, args.amp1, 2 * args.number)
    with open('input_dist.txt', 'w') as fp:
        for i, amp in enumerate(amps):
            fp.write(f'{i+1} {math.sqrt(amp * math.cos(args.angle))} {math.sqrt(amp * math.sin(args.angle))}\n')

