
\c deltasample;

-- Insert starting data
INSERT INTO Location (Id, Name) VALUES (3, 'Melbourne');

-- Insert
INSERT INTO Person (Id, LocationId, Name) VALUES (4, 2, 'Sam');
INSERT INTO Person (Id, LocationId, Name) VALUES (5, 3, 'King');

-- Update
UPDATE Person SET Name = 'Troy A.' WHERE Id = 3;
UPDATE Person SET LocationId = 3 WHERE Id = 3;
UPDATE Person SET Name = 'Mark B.' WHERE Id = 1;
UPDATE Location SET Name = 'Alice Spring' WHERE Id = 1;

-- Delete
DELETE FROM Person WHERE Id = 2