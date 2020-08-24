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

    amps = linspace(args.amp0, args.amp1, args.number)
    with open('input_dist.txt', 'w') as fp:
        for i, amp in enumerate(amps):
            rx = math.sqrt(amp * math.cos(args.angle))
            ry = math.sqrt(amp * math.sin(args.angle))

            # particle IDs
            pid = 2*i
            pid_twin = pid + 1
            fp.write('{:d} {:.10f} {:.10f}\n'.format(
                pid, rx, ry))
            fp.write('{:d} {:.10f} {:.10f}\n'.format(
                pid_twin, rx+1e-6, ry+1e-6))
