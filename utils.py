import re
import time

def to_float(value):
    value = f"{value}"
    pattern = r'\s|R\$'

    value = re.sub(pattern, '', value)

    if value == '-':
        return 0.0

    value = value.replace('.','')
    value = value.replace(',', '.')
    print("v", value)
    return float(value)

def to_currency(value):
    if value < 1e3:
        return f"{value:.2f}"
    elif value < 1e6:
        return f"{value / 1e3:.2f} K"
    elif value < 1e9:
        return f"{value / 1e6:.2f} M"
    elif value < 1e12:
        return f"{value / 1e9:.2f} B"
    else:
        return f"{value / 1e12:.2f} T"

def show_elapsed_time(start_time):
    elapsed_time = time.time() - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print()
    print("-----" * 10)
    print("--- %d Hours, %d Minutes and %.2f Seconds ---" % (hours, minutes, seconds))
    print("-----" * 10, "\n")