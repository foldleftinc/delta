-- Create database
CREATE DATABASE deltasample;

\c deltasample;

-- Creation of table Location
CREATE TABLE IF NOT EXISTS Location (
  Id INT NOT NULL,
  Name varchar(450) NOT NULL,
  PRIMARY KEY (Id)
);

-- Creation of table person
CREATE TABLE IF NOT EXISTS Person (
  Id INT NOT NULL,
  LocationId INT NOT NULL,
  Name varchar(250) NOT NULL,
  PRIMARY KEY (Id),
  CONSTRAINT fk_location
      FOREIGN KEY(LocationId)
	  REFERENCES Location(Id)
);

-- Set full replica identity
ALTER TABLE Person REPLICA IDENTITY FULL;
ALTER TABLE Location REPLICA IDENTITY FULL;

-- Insert starting data
INSERT INTO Location (Id, Name) VALUES (1, 'Sydney');
INSERT INTO Location (Id, Name) VALUES (2, 'Adelaide');

INSERT INTO Person (Id, LocationId, Name) VALUES (1, 1, 'Mark');
INSERT INTO Person (Id, LocationId, Name) VALUES (2, 1, 'Chinkit');
INSERT INTO Person (Id, LocationId, Name) VALUES (3, 1, 'Troy');