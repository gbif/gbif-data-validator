    
# Glossary
    
## Parser
A parser is a software component that takes input data (frequently text) and builds a data structure [1].

```
Text : "4" -> Integer : 4
```
    
## Interpreter
An interpreter is a software component that takes structured data as input and put it into a specific context with bouderies and specific meaning.

```
Integer : 4 -> minimumElevationInMeters : 4 meters
```

## Criterion
Criterion represents a rule to be evaluated on one or more data field.

```
minimumElevationInMeters > -5000 && minimumElevationInMeters < 5000
```

## Evaluation
Evaluation represents the evaluation of the criteria to produce a result.
Input data + Criterion produces an answer, the evaluation result. The type of the result is not strict, it can be boolean, numeric, String ... 

```
// with minimumElevationInMeters = 4;
boolean result = (minimumElevationInMeters > -5000 && minimumElevationInMeters < 5000);
```

## Validation
The interpretation of the answer/evaluation result. The interpretation is mostly tight to the concept of Fitness For Use where the severity or weight of the evaluation result will be quantified.

[1] "Parser." Wikipedia: The Free Encyclopedia. Wikimedia Foundation, Inc. Retrieved June 28, 2016, from https://en.wikipedia.org/wiki/Parsing#Parser
