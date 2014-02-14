#include <stdio.h>
#include <math.h>
#include <stdlib.h>

main(){

double lambda_min=100.0,lambda_max=30000.0,lambda_step=10.0;

int nfilters=5,ii;

char *filters[5];

for(ii=0;ii<nfilters;ii++)filters[ii]=new char[10];

sprintf(filters[0],"u");
sprintf(filters[1],"g");
sprintf(filters[2],"r");
sprintf(filters[3],"i");
sprintf(filters[4],"z");

double mu[5];

mu[0]=lambda_min+200.0*lambda_step;
mu[1]=mu[0]+300.0*lambda_step;
mu[2]=mu[1]+700.0*lambda_step;
mu[3]=mu[2]+700.0*lambda_step;
mu[4]=mu[3]+500.0*lambda_step;

double sigma[5];

sigma[0]=100.0*lambda_step;
sigma[1]=200.0*lambda_step;
sigma[2]=200.0*lambda_step;
sigma[3]=150.0*lambda_step;
sigma[4]=200.0*lambda_step;

double sb,norm,phi,ll;

FILE *bandpass,*answer;
char bname[100],aname[100];

for(ii=0;ii<nfilters;ii++){
    norm=0.0;
    sprintf(bname,"test_bandpass_%s.dat",filters[ii]);
    sprintf(aname,"test_phi_%s.dat",filters[ii]);
    
    bandpass=fopen(bname,"w");
    
    for(ll=lambda_min;ll<lambda_max+1.0;ll+=lambda_step){
        sb=exp(-0.5*(ll-mu[ii])*(ll-mu[ii])/(sigma[ii]*sigma[ii]));
        norm+=lambda_step*sb/ll;
        
        fprintf(bandpass,"%.18e %.18e\n",ll,sb);
    }
    
    fclose(bandpass);
    
    printf("norm %e\n",norm);
    
    answer=fopen(aname,"w");
    for(ll=lambda_min;ll<lambda_max+1.0;ll+=lambda_step){
        sb=exp(-0.5*(ll-mu[ii])*(ll-mu[ii])/(sigma[ii]*sigma[ii]));
        phi=sb/(ll*norm);
        
        fprintf(answer,"%.18e %.18e\n",ll,phi);
    }
    fclose(answer);
}

}
