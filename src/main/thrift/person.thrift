namespace java example.thrift

struct Address
{
        1: string Line1,
        2: string Line2
}

struct Person
{
        1: i32 Id,
        2: string Name,
        3: list<Address> Address
}
