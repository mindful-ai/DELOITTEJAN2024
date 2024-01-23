# Write a function checkprime which returns True if the number is prime and False otherwise

if __name__ == "__main__":
    n = int(input("Enter a number: "))
    if(checkprime(n)):
        print("The number is prime")
    else:
        print("The number is  not prime")