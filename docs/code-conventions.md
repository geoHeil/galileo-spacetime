Code Conventions
================

Stated extremely well by the Java coding standards document, code conventions
are important:

* 80% of the lifetime cost of a piece of software goes to maintenance.  Hardly any software is maintained for its whole life by the original author.
* Code conventions improve the readability of the software, allowing engineers to understand new code quickly and thoroughly.
* If you ship your source code as a product, it should be as well packaged and clean as any other product you create.

We try to follow the Java conventions in this project, while acknowledging that good coding conventions are *guidelines* and do not have to be strictly adhered to.  This document contains the exceptions to the rule, the guidelines that we *do* adhere to strictly, which may override some Java coding standards.

The Java code conventions are available here: http://www.oracle.com/technetwork/java/codeconv-138413.html

For Galileo, keep in mind:
* Four (4) spaces for each increment of indentation.  **NO TABS!**  This matches other CSU distributed systems group projects and makes viewing source files sane across editors.
* 80-column line lengths, unless readability will suffer greatly.  In general, if you have so many levels of indentation that this becomes unreasonable, your code probably needs refactoring anyway.
* Statements (if, while, for, etc) are padded with spaces.  ```if (something) { ... }```
* Spaces after commas (for arguments, lists, etc). ```myMethod(true, false, 3.6);```
* Opening braces go on the same line as the statement.  ```while (true) {``` else looks like ```} else {```

These points ensure a reasonable level of uniformity in the codebase.
