import random


class wordjumblegame(object):

    def __init__(self, name, wordlist):
        self.L = wordlist
        self.points = 0
        self.name = name
        self.start = 0
        self.end = 0
        

    def jumble(self, w):
        temp = list(w)
        random.shuffle(temp)
        return ''.join(temp)

    def run(self):
        random.shuffle(self.L)
        
        # Repeat until all words are done
        # Pick a word
        for word in self.L:

            # Jumble the word
            jword = self.jumble(word)

            # Show the word to the user
            print("Jumbled Word -> ", jword)

            # Ask for the user input
            uword = input("Your guess ? ")

            # Compare the user input with original word
            # Update points
            if(uword == word):
                self.points += 1
                print("Correct!")
            else:
                print("Incorrect..")   
             

    def showresults(self):
        # Print the results
        if(self.points > 6):
            print("Excellent Playing!")
        elif(3 <= self.points <= 6):
            print("Good")
        else:
            print("Improvement needed")

    def getresult(self):
        return self.points

if __name__ == "__main__":

    words = ["apples", "laptop", "mangoes", "computer", "mobile", "hyundai", "mercedes", "samsung"]

    playernames = ["Akshay", "Srinivas"]

    players = []
    for player in playernames:
        players.append(wordjumblegame(player, words))

    i = 1
    scores = {}
    for player in players:
        print("--------------", player.name, '-----------------')
        player.run()
        scores.setdefault(player.name, [player.getresult()])
        print("\n")

    print("FINAL SCORES: ")
    for key, value in scores.items():
        print(key.ljust(15), ' | ', value[0], '     | ', value[1])

    print("THANK YOU!")

    
# How much time each player took in-order to play the game?
