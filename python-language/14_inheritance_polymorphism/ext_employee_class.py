class Employee:

    # Class variables
    empCount = 0

    # Constructor and Data Intialization
    def __init__(self, n, company, salary):
        self.name = n
        self.company = company
        self.salary = salary
        self.tax = 0
        Employee.empCount += 1

    # Functions
    def printinfo(self):
        print(self.name.upper())
        print('-'*30)
        print('COMPANY    :', self.company)
        print('SALARY     :', self.salary)
        print('TAX        :', self.tax)
        

    def setsalary(self, newsalary):
        self.salary = newsalary
        self.calctax()

    def getsalary(self):
        return self.salary

    def calctax(self):
        self.tax = 0.1 * float(self.salary.split()[0])

#---------------------------------------------------------------

# CORE DEVELOPER -> Inheritance

class newemp(Employee):
    pass

#---------------------------------------------------------------

class extEmployee(Employee):

    # Class variables
    # empCount is already available here

    # Constructor and Data Initialization
    # Supposed to distribute the data between parent and child
    def __init__(self, n, age, company, salary):
        super().__init__(n, company, salary)
        self.age = age
        self.addr = ''

    # Functions

    # All functions of Employee is also available here

    # Overridden function
    def printinfo(self):
        super().printinfo()
        print('-'*30)
        print('AGE        :', self.age)
        print('ADDRESS    :', self.addr)
        print('\n\n')

    # New functions
    def setaddr(self, addr):
        self.addr = addr
