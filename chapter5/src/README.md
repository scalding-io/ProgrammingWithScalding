To understand the design patterns mentioned in this chapter first study :

    The External Operations pattern

This is the basic pattern that enables modularity and following patterns use. 

Then check:

    The Dependency Injection pattern

That allows us to inject mock dependencies in a Job. Two example implementations exist,
one using an injection at the construction of the Job and the other one uses the DI framework
'subcut'.

Finally check:

    The Late Bound pattern

All distributed systems (see Akka, Storm, Spark, Map-Reduce) require data to be serializable.
In the case we don't have a serializable object we can postpone the instantiation of objects 
using the late bound pattern.