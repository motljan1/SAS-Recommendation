/*** Normalise by User bias ***/
proc sql;
	create table reco.base_norm as
	select a.UserId
		 , a.ItemId
		 , a.rating+b.bias as Rating
		 , a.DevSample
	from reco.sample a
	join reco.average_user b
	on a.UserId = b.UserId;
quit;

/*** Sparse to dense ***/
proc iml;
/* Read data*/
use reco.base_norm;
read all var{Rating UserId ItemId} where(DevSample="L");
close;

/* combine UserId ItemId Rating into a matrix sparse */
sparse = Rating || UserId || ItemId;

/* Conversion */
dense = full(sparse);

/* Store data */
create reco.base_dense from dense;
append from dense;
close reco.base_dense;

quit;

/*** Replace zeros with missings ***/
data reco.base_imputed; 
set reco.base_dense;
array nums _numeric_;
 
do over nums;
 if nums=0 then nums=.;	
end;
run;

/* Item AVG to Missing rating */ /******************************************************************* null >>> ItemAVG */
proc iml;
use reco.base_imputed;
read all into rating;
close;
do item = 1 to ncol(rating);
itemAVG = /*mean(rating[ ,item])*/ sum(rating[ ,item])/countn(rating[ ,item]);
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


/*** SVD. See: http://www.cs.carleton.edu/cs_comps/0607/recommend/recommender/svd.html for more details ***/
proc princomp data=reco.BASE_DENSE_AVGED
	out=reco.base_svd
	outstat=reco.base_svd_score
	noprint
	cov 
	noint
	n=20;
    var Col1-Col1500;   
run;

proc iml;
	/* Read data */
	use reco.base_svd;
	   read all var _NUM_ into princ[colname=NumerNames];
	close;

	use reco.base_svd_score;
	   read all var _NUM_ into score[colname=NumerNames];
	close;
	 
	/* Select only useful data from the input */
	length = ncol(princ);
	princ = princ[ , length-20+1:length];
	length = nrow(score);
	score = score[length-20+1:length, ];
	
	/* Matrix multiplication */
	xhat = princ * score;

	/* Dense to sparse */
	output = sparse(xhat);
	 
	/* Store data */
	create reco.svd from output;
	append from output;
	close reco.svd;
quit;


/*** Rename columns ***/
proc datasets library=reco nolist;
modify svd;
rename Col1 = PredRating;
rename Col2 = UserId;
rename Col3 = ItemId;
quit;

/*** Normalise by UserItem bias ***/
proc sql;
	create table reco.svd as
	select a.UserId
		 , a.ItemId
		 , a.PredRating-b.bias as PredRating
	from reco.svd a
	join reco.average_user b
	on a.UserId = b.UserId;
quit;

/* Replace missings & bind to limits */
data reco.svd; 
set reco.svd;
ImputedRating = PredRating;
if ImputedRating = . then ImputedRating = 3.53;
PredRating = min(max(ImputedRating, 1), 5);
run;

