# SqliteWrapper

[![][nuget-img]][nuget]

[nuget]:     https://www.nuget.org/packages/SqliteHelper/
[nuget-img]: https://badge.fury.io/nu/Object.svg

Simple database wrapper for Sqlite written in C#.  

## Description

SqliteWrapper is a simple database wrapper for Sqlite databases written in C#.   

Core features:
- dynamic query building using expression objects
- support for nested queries within expressions
- support for SELECT, INSERT, UPDATE, and DELETE, or raw queries
- built-in sanitization

## Important

For .NET Framework, you may need to add the ```System.Data.SQLite.Core``` NuGet package, clean, and then re-build.  This should place ```x86``` and ```x64``` folders in your build output.

This is particularly helpful if you are experiencing the exception ```System.DllNotFoundException: Unable to load DLL 'SQLite.Interop.dll': The specified module could not be found.```

## Help and Support

Please contact me for any issues or enhancement requests!  I'm at joel dot christner at gmail dot com.  

## New in v1.1.2

- Added table management capabilities (create, drop, describe)

## A Note on Sanitization

Use of parameterized queries vs building queries dynamically is a sensitive subject.  Proponents of parameterized queries have data on their side - that parameterization does the right thing to prevent SQL injection and other issues.  *I do not disagree with them*.  However, it is worth noting that with proper care, you CAN build systems that allow you to dynamically build queries, and you SHOULD do so as long as you build in the appropriate safeguards.

If you find an injection attack that will defeat the sanitization layer built into this project, please let me know!

## Simple Example

Refer to projects SampleApp, TestNetFramework, and TestNetCore for examples.

## Select with Pagination

Use indexStart, maxResults, and orderByClause to retrieve paginated results.  The query will retrieve maxResults records starting at row number indexStart using an ordering based on orderByClause.  See the example in the SampleApp project.

## Version History

Please refer to CHANGELOG.md.
