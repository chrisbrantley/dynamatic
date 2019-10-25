# Dynamatic

Dynamatic is a python library for working with DynamoDB. It is NOT an ORM. It sits somewhere between the low-level [boto3](https://github.com/boto/boto3) library (on which Dynamatic is based) and high-level ORMs like [PynamoDB](https://github.com/pynamodb/PynamoDB).

## Motivation

First, I was inspired by [this blog post](https://www.trek10.com/blog/dynamodb-single-table-relational-modeling/) which was inspired by [this excellent re:Invent 2018 session by Rick Houlihan](https://youtu.be/HaEPXoXVf2k) about how to properly model data in DynamoDB for efficiency and scale.

Second, I did not want an ORM. ORMs like PynamoDB package their own methods of defining object schemas but there are already excellent libraries out there like [Pydantic](https://github.com/samuelcolvin/pydantic) or [Marshmallow](https://github.com/marshmallow-code/marshmallow). They also tend to impose traditional relational database patterns (like Active Record) that do not scale well.

Third, boto3 is a great library but its DynamoDB API is painfully low-level and not particularly "Pythonic". Working with advanced features like [Update Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html) and [Projection Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html) is complex and unintutive.

So this library aims to be a happy medium: Providing enough abstraction and "developer ergonomics" to get up and running with DynamoDB quickly, while not getting in the way with high-level ORM features.

## Installation

Dynamatic requires Python 3.6 or later. You can install it via pip.

```bash
pip install dynamatic
```

## Usage

TODO! Documentation and usage examples coming soon!

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
