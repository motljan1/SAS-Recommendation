/**************************************************************************************/
LIBNAME reco'/folders/myfolders/KNN/Data'; 		/* Data directory specification       */  
%let InDS= reco._base;							/* Basic DataSet                      */
%let RandomSeed = 955;							/* Dividing random number seed)       */
%let k=80; 	/* default 50 */					/* Count of nearest neighbors to find */ 
%let DistanceMethod=cosine;						/* Distance measure method	  		  */
/**************************************************************************************/



/*** Sampling - divide to training (L) and testing(T) ***/
data reco.Sample;
    set &InDS;
    if _n_=1 then
        call streaminit(&randomseed);
    U=rand('uniform');
    if U<=0.80 
	then DevSample='L';
    else DevSample='T';
run;







/*Max & Min Ratings*/
Title3 "Max Rating";
Proc sql noprint;
select max(rating)
into: MaxRating
from &InDS;
quit;

Title3 "Min Rating";
Proc sql noprint;
select min(rating)
into: MinRating
from &InDS;
quit;



/*AVG Rating to variable: AvgRating */
Title3 "AVG DataSet Rating";
proc sql;
SELECT AVG(Rating) 
Into: AvgRating 
FROM reco.Sample
where DevSample= "L";
quit;

/*AVG Item Rating */
proc sql;
create table reco.AVG_ItemID as 
select ItemID, avg (Rating) as AvgRatingOnItem
from reco.Sample
where DevSample = "L" group by ItemID;
quit;

/*AVG User Rating */
proc sql;
create table reco.AVG_UserID as 
select UserID, avg (Rating) as AvgUserRating, avg (Rating) - &AvgRating as Bias
from reco.Sample 
where DevSample = "L" group by UserID;
quit;








/*** Sparse to dense ***/
proc iml;
/* Read data*/
use reco.sample;
read all var{Rating UserId ItemId} where(DevSample="L");
close;
/* combine UserId ItemId Rating into a sparse matrix */
/*       Value	||	Row	  ||  Col     				 */
sparse = Rating || UserId || ItemId;

/* Conversion sparse to dense matrix*/
/*	|		ItemID	ItemID |		*/
/*	|UserID				   |		*/
/*	|UserID			Rating |        */
dense = full(sparse);
/* Store data */
create reco.base_dense from dense;
append from dense;
close reco.base_dense;
quit;

/*** Rating: Set missing values instead 0 ***/
data reco.base_dense_null; 
set reco.base_dense;
array nums _numeric_;
 
do over nums;
 if nums=0 then nums=.;	
end;
run;







/* Rating normalization:  Substract userAVG from rating ************************************************    NORMALIZATION      */
/* base_dense_normalized OUT */
/* base_dense_null       IN  */
proc iml;
use reco.base_dense_null;
read all into ratings;
close;

use reco.AVG_ItemID;
read all into avgs;
close;

do user = 1 to nrow(ratings);
	do item = 1 to ncol(ratings);
		if ratings [user,item] ^=. then do; 
			ratings [user,item] = ratings [user,item] - avgs [user, 2];
		end;
	end;
end;

create reco.base_dense_normalized from ratings ;
append from ratings ;
close reco.base_dense_normalized ;
quit;



/* Item AVG to Missing rating */   /*******************************************************************    null >>> ItemAVG     */
/* base_dense_avged       OUT */
/* base_dense_null        IN  */
proc iml;
use reco.base_dense_null;
read all into rating;
close;

do item = 1 to ncol(rating);
	itemAVG =  /*mean(rating[ ,item])*/  sum(rating[ ,item])/countn(rating[ ,item]);
	
do replacement = 1 to nrow(rating);
	
	if rating [replacement ,item] =. then do;
			rating [replacement ,item] = itemAVG ;
			end;
	end;
end;

create reco.base_dense_avged from rating ;
append from rating ;
close reco.base_dense_avged;
quit;

/* Replace missing when no one has ever watched the movie */
data reco.base_dense_avged;
set reco.base_dense_avged;
array nums _numeric_;
do over nums;
 if nums=. then nums=&AvgRating;
end;
run;






/* Ridit – transform rating to 0-1 interval ************************************************************         RIDIT    */
/* base_dense_ridit       OUT */
/* base_dense_null        IN  */


proc iml;
use reco.base_dense_null;
read all into ratings;
close;

do usr = 1 to nrow(ratings);

c1=0; c2=0; c3=0; c4=0; c5=0;
	
	do rating = 1 to ncol(ratings);
		if ratings [usr,rating] = 1 then do;
			c1 = c1 + 1; end;
		if ratings [usr,rating] = 2 then do;
			c2 = c2 + 2; end;
		if ratings [usr,rating] = 3 then do;
			c3 = c3 + 3; end;
		if ratings [usr,rating] = 4 then do;
			c4 = c4 + 4; end;
		if ratings [usr,rating] = 5 then do;
			c5 = c5 + 5; end;
	end;
	
	rsum = sum(c1,c2,c3,c4,c5);
	w1 = 0.5*(c1/rsum);
	w2 = 0.5*(c2/rsum) + (c1/rsum);
	w3 = 0.5*(c3/rsum) + (c2/rsum) + (c1/rsum);
	w4 = 0.5*(c4/rsum) + (c3/rsum) + (c2/rsum) + (c1/rsum);
	w5 = 0.5*(c5/rsum) + (c4/rsum) + (c3/rsum) + (c2/rsum) + (c1/rsum);

	do rat = 1 to ncol(ratings);
		if ratings [usr,rat] = 1 then do;
			ratings [usr,rat] = w1; end;
		if ratings [usr,rat] = 2 then do;
			ratings [usr,rat] = w2; end;
		if ratings [usr,rat] = 3 then do;
			ratings [usr,rat] = w3; end;
		if ratings [usr,rat] = 4 then do;
			ratings [usr,rat] = w4; end;
		if ratings [usr,rat] = 5 then do;
			ratings [usr,rat] = w5; end;
	end;
end;	
		
create reco.base_dense_ridit from ratings ;
append from ratings ;
close reco.base_dense_ridit;
quit;




/* Users distances ************************************************************************************        DISTANCE    */
proc distance 
	SHAPE=SQR
	REPONLY
	data=reco.base_dense_ridit /* ridit*/ /* avged */ /* normalized  */
	method= &DistanceMethod 
	out=reco.distance;
    var ratio /*interval*/ (Col1-Col1682);
   run;
   
/* Remove diagonal distances */
proc iml;
use reco.distance;
read all into inputData;
close;

do d = 1 to ncol(inputData);
	inputData[d,d] =. ;
end;

create reco.distance_diag from inputData;
append from inputData;
close reco.distance_diag ;
quit;








/*** k-NN ***************************************************************************************************        k-NN       */
proc iml;
/* Read data */
use reco.base_dense_avged ;
read all var _num_ into rating;
close;


use reco.distance_diag;
read all var _all_ into distances;
close;

/* Settings */
k = &k; 

/* Initialisation */
nearestNeighbor = J(nrow(rating ), k, 0); 			/* Matrix of nearest neighbors */
recommendation = J(nrow(rating), ncol(rating), 0);	/* Matrix with the estimated rating */
distance = J(nrow(rating ), 1, 0 ) /*10**10*/; 	        /* Vector of distances *************************************************************/

/* Loop - get the nearest neighbours */
do reference = 1 to nrow(rating );
	
	distance = distances[ ,reference];
	
	/* Sort distances in ascending (1–N) order, return indices */
	call sortndx(ndx, distance, {1}, {1} /*,descend*/); 
	
	/* Store k nearest neighbours */
	nearestNeighbor[reference,] = ndx[1:k]`; /**/

	/* Get recommendation (average recommendation of the nearest neighbours) */
	recommendation[reference,] = mean(rating[nearestNeighbor[reference,],]);
		
end;	

/* Convert dense to sparse matrix */
result = sparse(recommendation); 

/* Store data */
create reco.knn_all from result;
append from result;
close reco.knn_all;
quit;


/* Rename columns */
proc datasets library=reco nolist;
modify knn_all;
rename Col1 = PredRating;
rename Col2 = UserId;
rename Col3 = ItemId;
quit;





/* k-NN result balancing: prepare tables */
proc sql;
create table reco.knn_all_debiased as
select 
k.PredRating,
a.Bias,
k.UserID, 
k.ItemID
From reco.knn_all k 
left join reco.AVG_UserID a
on k.userID = a.userID;
quit;

/* k-NN result balancing */
proc sql;
create table reco.knn_all_debiased_II as
select 
PredRating + Bias as PredRating,
UserID, 
ItemID
From reco.knn_all_debiased ;
quit;


/*** Bound to limits ***/
data reco.knn_all_debiased_II;
set reco.knn_all_debiased_II ;
    PredRatingBounded= 
	min(max(PredRating, &MinRating ),&MaxRating);
run;






/******************************************************/
/********************* EVALUATION *********************/
/******************************************************/




/* Sort */
proc sort data=reco.Sample;
by UserId ItemId;
run;


 
/* Data to evaluate*/
%let PredRating = PredRatingBounded;		/* PredRatingBounded */
%let tableName  = reco.knn_all_debiased_II; /* reco.knn_all_debiased_II */

/* Tell SAS that the table is sorted to accelerate subsequent queries */
proc sort data=&tableName presorted; 
by UserId ItemId;
run;
 
/* Merge target and prediction & calculate Square Error */
data reco.rmse_merged(keep=SquareDiff);
     merge reco.sample(keep=UserId ItemID Rating DevSample where=(DevSample="T") in=a)
           &tableName(keep=UserId ItemID &PredRating in=b);
     by UserId ItemID;
     if a & b;
     SquareDiff = (Rating-&PredRating)*(Rating-&PredRating);
     Diff 		= round(sqrt((Rating-&PredRating )*(Rating-&PredRating )));
run;

/* Save rounded Predictions */
proc sql;
create table reco.rmse_success as
	select round(sqrt(SquareDiff)) as Diff
	from reco.rmse_merged;
quit;


Title3 "RMSE";
/* Print RMSE */
proc sql;
select sqrt(avg(SquareDiff))
into: RMSE
from reco.rmse_merged;
quit;


/* Report Prediction Succes */
Title3 "Differences: Success, Diff1-Diff4, SUM";
proc iml;
use reco.rmse_success ;
read all var {Diff} into diff;
close; 

t = countn (diff);

counts = J(2, 6, 0);

	do d = 1 to 5;
		c=0;
		do dr = 1 to nrow(diff);
			if diff[dr] = d-1 then do;
				c = c+1;
			end;
		end;
		
		p = 100 * c / t;
		counts[1,d] = c;
		counts[2,d] = p;	
	end;
counts[1,6] = sum (counts [1, ]);
counts[2,6] = sum (counts [2, ]);
print counts;
quit;
















