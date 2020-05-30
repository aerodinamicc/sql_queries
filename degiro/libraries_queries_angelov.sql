-----------------------
--- 1. How many copies of the book titled The Lost Tribe are owned by the library branch whose name is "Sharpstown"?
-----------------------

SELECT
	library_branch.branchname,
	book.title,
	book_copies.no_of_copies
FROM library_branch
JOIN book_copies using(branchid)
JOIN book using(bookid)
WHERE book.title = 'The Lost Tribe'
	AND library_branch.branchname = 'Sharpstown'
	
-----------------------
--- 2. How many copies of the book titled The Lost Tribe are owned by each library branch?
-----------------------

SELECT
	library_branch.branchname,
	book.title,
	book_copies.no_of_copies
FROM library_branch
JOIN book_copies using(branchid)
JOIN book using(bookid)
WHERE book.title = 'The Lost Tribe'

-----------------------
--- 3. Retrieve the names of all borrowers who do not have any books checked out .
-----------------------


SELECT
	borrower.name
FROM borrower
--- NB: Don't have any books checked out at the moment
WHERE borrower.cardno NOT IN ( SELECT DISTINCT cardno FROM book_loans WHERE date(DueDate) >= now()::date)

-----------------------
--- 4. For each book that is loaned out from the "Sharpstown" branch and whose DueDate is today, 
--- retrieve the book title, the borrower's name, and the borrower's address.
-----------------------

SELECT
	book.title,
	borrower.name,
	borrower.address
FROM book_loans
JOIN library_branch using(branchid)
JOIN book using(bookid)
JOIN borrower using(cardno)
WHERE date(book_loans.DueDate) = now()::date
	AND library_branch.branchname = 'Sharpstown'
	
-----------------------
--- 5. For each library branch, retrieve the branch name and the total number of books loaned out from that branch.
-----------------------

SELECT
	library_branch.branchname,
	COUNT(*) as loaned_books
FROM library_branch
JOIN book_loans using(branchid)
-- NB: Books out currently 
WHERE date(book_loans.DueDate) > now()::date 
GROUP BY 1
ORDER BY 1
	
-----------------------
--- 6. Retrieve the names, addresses, and number of books checked out for all borrowers who have more than five books checked out. 
-----------------------

--Assuming nobody had two different cardno

SELECT 
	borrower.name,
	borrower.address
	count(*) AS books_due
FROM borrower
JOIN book_loans using(cardno)
WHERE date(book_loans.DueDate) > now()::date 
GROUP BY 1, 2
HAVING count(*) > 5

-----------------------
--- 7. For each book authored (or co-authored) by "Stephen King", 
--- retrieve the title and the number of copies owned by the library branch whose name is "Central"
-----------------------

SELECT
	library_branch.branchname,
	book.title,
	SUM(book_copies.no_of_copies) as no_of_copies
FROM library_branch
JOIN book_copies using(branchid)
JOIN book 
	ON book.bookid = book_copies.bookid
JOIN book_authors
	ON book.bookid = book_authors.bookid
WHERE book_author.authorname LIKE '%Stephen King%'
	AND library_branch.branchname = 'Central'
GROUP BY 1, 2

