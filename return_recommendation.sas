** Get top n recommendations ;
proc sort data=reco.pls;
  by UserId descending PredRating;
run;
 
data reco.top_five;
  set reco.pls;
  retain counter;
  if (UserId ^= lag(UserId)) then counter = 0;
  counter + 1;
  if counter <= 2;
run;


PROC SQL;
   CREATE TABLE reco.top AS 
   SELECT t1.UserId, 
          t1.ItemId, 
          t2.Name, 
          t1.PredRating
      FROM RECO.TOP_FIVE t1
           INNER JOIN RECO.ITEM t2 ON (t1.ItemId = t2.ItemId)
	order by UserId,  PredRating desc;
QUIT;
