class NegativeNumberException(Exception):

    def __init__(self, message):
        super().__init__(message)



# ------------------------------------------


def nfactorial(n):
    if n <= 0:
        raise NegativeNumberException('Negative numbers not allowed for factorial')
    else:
        if(n == 1):
            return 1
        else:
            return n * nfactorial(n - 1)


# -----------------------------------------


n = int(input('Enter a number: '))
try:
    print(nfactorial(n))
except Exception as E:
    print('There was an exeception')
    print(E)
