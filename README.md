# SqliteWrapper

[![][nuget-img]][nuget]

[nuget]:     https://www.nuget.org/packages/SqliteHelper/
[nuget-img]: https://badge.fury.io/nu/Object.svg

Simple database wrapper for Sqlite written in C#.  

For a sample app exercising this library, refer to the test project contained within the solution.

## Description
SqliteWrapper is a simple database wrapper for Sqlite databases written in C#.   

Core features:
- dynamic query building using expression objects
- support for nested queries within expressions
- support for SELECT, INSERT, UPDATE, and DELETE, or raw queries
- built-in sanitization

## Help and Support
Please contact me for any issues or enhancement requests!  I'm at joel at maraudersoftware dot com.  This is an early release and it works well, however, I need to spend more time on performance.  

## Installing with NuGet
Due to some unforseen issues with NuGet, you must download and add sqlite3.dll to your project manually.  Set it to copy to output always.

## New in v1.0.0
- Initial release

## A Note on Sanitization
Use of parameterized queries vs building queries dynamically is a sensitive subject.  Proponents of parameterized queries have data on their side - that parameterization does the right thing to prevent SQL injection and other issues.  *I do not disagree with them*.  However, it is worth noting that with proper care, you CAN build systems that allow you to dynamically build queries, and you SHOULD do so as long as you build in the appropriate safeguards.

If you find an injection attack that will defeat the sanitization layer built into this project, please let me know!

## Simple Example
Refer to the SampleApp project for a complete example.

## Select with Pagination
Use indexStart, maxResults, and orderByClause to retrieve paginated results.  The query will retrieve maxResults records starting at row number indexStart using an ordering based on orderByClause.  See the example in the SampleApp project.

## Running under Mono
This library uses Mono.Data.Sqlite which requires sqlite3.dll.  sqlite3.dll has been manually added to each project with its copy setting set to "always copy".  You may want to use the Mono AOT (ahead of time) compiler prior to using any binary that includes this library on Mono.
