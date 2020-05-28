#!/usr/bin/perl
use DBI;
use strict;
use FileHandle;
use Net::FTP;
use Date::Calc qw(:all);
use Data::Dumper qw(Dumper);
use List::Util qw(max min);

####常用变量
#===============================================================================
my $AUTO_HOME = $ENV{"AUTO_HOME"};      
my $AUTO_DSN  = $ENV{"AUTO_DSN"};
my $Province=862; #甘肃编码  
my $ConStr="\x05";  #分隔符	
my $PARALLEL_BWT = "32";
my $PARALLEL_MAX = "12";
my $PARALLEL_NORMAL = "8";
my $PARALLEL_MIN = "4";
my $record="0x0d0x0a";
#获取调度执行日期
my $CONTROL_FILE = $ARGV[0];
my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8); #数据日期

######################################################################
#以下变量需要新开发人员自己填写，其他变量勿动
#==============================	start	================================
my $RERUM="00";#批次号不建议修改，仅在特殊重传情况下使用
my $Job_Type       ='D';#用于日月接口区分
my $Submit_Num     =8000000;       #新导出程序目前只能按照据记录数分割文件不能按照文件大小分割文件，必须准确测算配置接近800MB文件的记录数。
my $Split_Size     =800000000;    #集团要求1GB分割，此处取近似800MB
my $TABLE='ODSPMART.T_BWT_DAPD_CLOUDACC_CUST_REL';
my $Group_File_Name='DAPD_CLOUDACC_CUST_REL';
my $sqlstr="SELECT /*+PARALLEL(S,${PARALLEL_MAX})*/ ".
					 "LATN_ID,CLOUDACC_ID,OWN_CUST_ID,PROD_INST_ID,CREATE_DATE,STATUS_CD ".
					 "FROM ODSPMART.T_BWT_DAPD_CLOUDACC_CUST_REL S  ".
					 "WHERE DAY_ID =$TX_DATE";  
my $sqlnum="SELECT /*+PARALLEL(S,${PARALLEL_MAX})*/ COUNT(*) FROM ODSPMART.T_BWT_DAPD_CLOUDACC_CUST_REL S WHERE DAY_ID =$TX_DATE";										 
#================================	end	================================
######################################################################
my $PutData_Path="E:/PUT_JT_DATA/FTP_DATA_PATH/${Group_File_Name}/"; 
my $LOGON_FILE = "${AUTO_HOME}/etc/EDA_LOGIN_ODSKF";
my $Conf_Path='E:\PUT_JT_DATA\theScript\Path.conf';
my $ROW_NUM=0;
my $TX_DATE="";
my $TABLE_NAME_LOG="";
my $LOGON_STR='';
my $signal=0;
my $targetDir='';
#===============================================================================	

sub main(){
	my $start_time=time();#程序开始时间
	print "开始时间:".Get_Time(1)."\n";
	&displayHeadInfo();
	&initVariable();
	&unlinkFile($PutData_Path);
	&expData();
	&modifyName();
	&builderValFile();
	&createCheck();
	&Gzip_DatFile($PutData_Path);
	&moveData();
	my $end_time=time();#程序结束时间
	print "\n结束时间:".Get_Time(1)."\n";
	printf("\n====================总耗时 %.1f 分钟  ====================\n",(($end_time-$start_time)/60)); 
}

main();

sub displayHeadInfo
{
	my $theDayDate=Get_Time(2); #调用Get_Time()函数获取当前日期
	print "###############################################################################\n";
	print "##            The Process No. is $$ || Today is on $theDayDate              ##\n";
	print "===============================================================================\n";	
	print "##                               ==START==                                   ##\n";
	print "###############################################################################\n";	
}

sub unlinkFile{
	print "\n==================== 【1】执行 删除目录中之前生成的文件 函数 ====================\n";  
	my $dirName=shift @_;
	print ">>>>>>>>".$dirName."\n";
	if(-e $dirName){
		opendir(DIRFILE,$dirName) or die "can't open the directory!";
	my @files=readdir(DIRFILE);
	foreach (@files){
		my $path=$dirName.$_;
		if(-f $path){
			if($_=~/${TX_DATE}/ && $_=~/$Group_File_Name/){
				unlink($path);
			}
		}
	}
	close DIRFILE;	
}else{
		`mkdir $dirName`;
 }
 print "删除目录中当前账期历史文件完成......\n";
}

sub expData{
	print "\n==================== 【2】执行导出Exp_DAT函数 ====================\n";	 
  print "导出数据脚本:\n$sqlstr\n\n";						 
  my $logfile="E:/ETL/LOG/BWT/".Get_Time(3)."/$TABLE_NAME_LOG.sqlulder2.log";		
  my $expFile="";
  if($Job_Type eq 'D'){
		$expFile="$PutData_Path".$Group_File_Name.".%y%m%d.".$TX_DATE.".${RERUM}.001.%B.".$Province.".DAT";
	}else{
	  $expFile="$PutData_Path".$Group_File_Name.".%y%m.".$TX_DATE.".${RERUM}.001.%B.".$Province.".DAT";
	}
	my ($dbstr,$DB_Connect,$orauser,$passwd)=dataBaseStr();
  my $cmdStr=`E:/ETL/APP/sqluldr2/sqluldr2.exe user="${dbstr}" query="${sqlstr}" field="${ConStr}" record="${record}" file="${expFile}" rows="${Submit_Num}" batch=yes mode=APPEND  safe=yes`;
	print("数据导出完毕...\n");
}



sub modifyName 
{	
	print "\n==================== 【3】执行 对导出的数据文件进行更名 函数 ====================\n";  
  my $dir="$PutData_Path";
  chdir($dir) or die "无法切换目录到 $dir , $!";
  opendir(DIRFILE,$dir) or die "can't open the directory!";
	my @files=readdir(DIRFILE);
	foreach my $f (@files){
		my $path=$dir.$f;
		if(-f $path){
	   	if($f=~/^*.DAT$/){
	   	  	rename($path,modifySerail($path));
	   	}
	  }
	}
	close DIRFILE;
	print("修改文件名称完毕......"."\n");
}

#=================================================
sub builderValFile{
		print "\n==================== 【4】执行 生成VAL文件 函数 ====================\n";  	
		my $dir="$PutData_Path";
		my $Total_row=0;
		opendir(DIRFILE,$dir) or die "can't open the directory,$!";
		my @files=readdir(DIRFILE);
		foreach my $f (@files){
			my @valconent=();
			if($f=~/.DAT/){
			my $path=$dir.$f;
			if(-f $path){
				push @valconent , $f;
				my @args = stat ($path);
				my $size=$args[7];
				push @valconent , $size;
				$f=~s/.DAT/.VAL/g;
				print "$f\n";
				my $fileRow=countRow($path);
				push @valconent , $fileRow;
				push @valconent,$TX_DATE;
				push @valconent,Get_Time(4);
				my $valstr = $valconent[0].$ConStr.$valconent[1].$ConStr.$valconent[2].$ConStr.$valconent[3].$ConStr.$valconent[4];
				
				open(HANDLE, ">>${dir}${f}");
				print HANDLE ($valstr."\n");
				close(HANDLE);
				
			  $Total_row+=$fileRow;
				print "当前文件记录数:${fileRow}--累计文件记录数:$Total_row\n";
				}
			}
		}
		close DIRFILE;
		
		if($ROW_NUM!=$Total_row){
			die "程序错误，数据库记录数与文件记录数不一致,请处理异常数据!\n";
		}
}


#=================================================
sub createCheck{
	print "\n==================== 【5】执行 根据导出的数据文件创建 CHECK 文件 函数 ====================\n";  
 	my $dir="$PutData_Path";
	my $checkName=$Group_File_Name.'.'.Get_Time(3).'.'.$TX_DATE.".${RERUM}.000.000.".$Province.'.CHECK';
	my @dats;
	opendir(DIRFILE,$dir) or die "can't open the directory,$!";
	my @files=readdir(DIRFILE);
	foreach my $f (@files){
		if($f=~/.DAT/){
					my $path=$dir.$f;
		if(-f $path){
	   	push(@dats,$f);
	  }
		}
	}
	close DIRFILE;
	
 open (CFILE,">".$dir.$checkName) or die "can't open the fiels,$!";
 	  foreach my $dt (@dats){
   		print CFILE $dt."\n";
 	  }
 	close CFILE;
	print("创建CHECK $checkName 文件完毕......"."\n");
}

sub Gzip_DatFile{
	print "\n==================== 【6】执行对数据文件进行压缩函数 ====================\n";  
	chdir(@_[0]);
	`gzip -f *.DAT`;
	print("DAT 文件压缩完成....."."\n");
}

sub moveData{
  	print "\n==================== 【7】移动数据到正式目录 ====================\n";  	
  	my $dir=$PutData_Path;
  	chdir( $dir ) or die "无法切换目录到 $dir , $!";
  	opendir(DIRFILE,$dir) or die "can't open the directory,$!";
		my @files=readdir(DIRFILE);
		foreach my $f (@files){
				my $path=$dir.$f;
			if(-f $path){
		   	if($f=~/${TX_DATE}/ && $f=~/$Group_File_Name/){
		   		`mv $path ${targetDir}`;
		   	}
		  }
		}
	 close DIRFILE;
  	print "文件移动至${targetDir}目录";
}

sub outputVariable{
	printf("\n==================== 以下为参数列表:  ====================\n");
	print "数据账期:$TX_DATE\n";
	print "HOME=$AUTO_HOME\n";
	print "Province=$Province\n";
	print "PutData_Path=$PutData_Path\n";
	print "RERUM=$RERUM\n";
}

sub initVariable{
  my $dirName=shift(@ARGV);  
	my ($sys,$path,$txt_date)= processArgv($dirName);
	print "获取调度参数:$dirName\n";
	$TX_DATE=$txt_date;
	my @tem= split /\./ ,$dirName;
	$TABLE_NAME_LOG=$tem[0];
	&outputVariable();
	
	if($Job_Type eq 'D'){
	  $targetDir=	"E:/PUT_JT_DATA/theData/DayData/"
	}else{
		$targetDir="E:/PUT_JT_DATA/theData/MonthData/";
	}
	
	print "TABLE_NAME_LOG=$TABLE_NAME_LOG\n";
	print "TABLE=$TABLE\n";
	print "Group_File_Name=$Group_File_Name\n";
	totalRow();
}

sub modifySerail{
	my ($name,$cdate,$ddate,$resendno,$pcno,$serial,$prov,$ftype)=split(/\./,@_[0]);
	$serial=sprintf("%03d",$serial);
	return $name.".".$cdate.".".$ddate.".".$resendno.".".$pcno.".".$serial.".".$prov.".".$ftype;
}

sub countRow{
	my $file_path=shift @_;
	open(FILE ,$file_path); 
	my $lines_counter = 0;  
	while(<FILE>)
	{
		$lines_counter += 1; 
	}
	close FILE;
	return ($lines_counter);
}

sub totalRow{
	my ($dbstr,$DB_Connect,$orauser,$passwd)=dataBaseStr();
	my $dbh;
	my $DB_Connect = "dbi:Oracle:GSEDA";
	$dbh = DBI->connect($DB_Connect,$orauser,$passwd,{ AutoCommit => 1, PrintError => 1, RaiseError => 0 , mysql_client_found_rows => 0 } ) ;
	unless ( defined($dbh) ) { print "database connect failed!\n";return undef; }
	my $stmt = $dbh->prepare($sqlnum) || die "Error: $DBI::errstr\n";
  $stmt->execute()|| die "Error: $DBI::errstr\n";
  my $rows =$stmt->fetchrow();
	$stmt->finish();
  $dbh->disconnect();
  $ROW_NUM=$rows;
  print "查询到${ROW_NUM}条记录数.\n";
  if($ROW_NUM==0){
  	#print "查询到0条记录，程序退出，请及时检查数据";
  	die "请注意这是一条报错信息:查询到0条记录，程序退出，请及时检查数据";
  }
}


sub processArgv{
	 my $str=shift @_;
   my @splitstr=();
   	while($str=~/\_/g){
		my $p=pos($str);
		push(@splitstr,$p);
	}
	my $max_val=max @splitstr;
	my $min_val=min @splitstr;
	my @arr=split (/\_/ ,$str);
	my ($sys,$path,$txt_date)=($arr[0],substr ($str,($min_val),($max_val-5)),substr(${str},length(${str})-12, 8));
	return ($sys,$path,$txt_date);
}

sub dataBaseStr{
	my $os = $^O;
  my $ora_odbc=$ENV{"AUTO_ORADSN"};
  if (!defined($ora_odbc)){
    $ora_odbc="GSEDA";
  }
  $os =~ tr [A-Z][a-z];
  my $DB_Connect = "dbi:Oracle:GSEDA";

  unless ( open(LOGONFILE_H, "${LOGON_FILE}") ) {
    print "Open Logon file fail, LOGON_FILE=${LOGON_FILE}\n";
    exit(253);
  }
  my $list = <LOGONFILE_H>;
  
  close(LOGONFILE_H);

  # Get the decoded logon string
  $list = `${AUTO_HOME}/bin/IceCode.exe "$list"`;
  my @list = split(/ /,$list);
  chop($list[1]);
  my ($orauser,$passwd)=split(/,/,$list[1]);
  my $LOGON_STR="$orauser\/$passwd\@GSEDA";
  return ($LOGON_STR,$DB_Connect,$orauser,$passwd);
}

#=================================================
sub Get_Time	#获取当前日期
#=================================================
{
	#print " == 0 == 	Start Get_Time :: 获取当前日期\n";	
	#flag: 1:获取当前日期+时间,2:#获取当前日期,3:获取当前时间
	my ($flag)=@_;                                                
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
	$year += 1900;
	$mon+=1;
	if ($mon < 10)  { $mon="0".$mon;  } else { $mon=$mon;}
	if ($mday < 10) { $mday="0".$mday; } else { $mday=$mday; }
	my $theSysTime1=sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$hour,$min,$sec); #获取当前日期+时间
	my $theSysTime2=sprintf("%4d-%02d-%02d %02d:%02d:%02d",$year,$mon,$mday,$hour,$min,$sec); #获取当前日期+时间
	my $Sys_Date=sprintf("%4d-%02d-%02d",$year,$mon,$mday);    #获取当前日期 YYYY-MM-DD
	my $Sys_Time=sprintf("%02d:%02d:%02d",$hour,$min,$sec);    #获取当前时间
	my $SysDate=sprintf("%4d%02d%02d",$year,$mon,$mday);    #获取当前日期    YYYYMMDD
	#print " == 1 == 	End Get_Time\n";
	if($flag == 4){return ($theSysTime1);}
	if($flag == 1)
		{return ($theSysTime2);}
	else
		{
			if($flag == 2)
			{return ($Sys_Date);}
			else
			{
				if($flag == 3)
				{return ($SysDate);}
				else
				{return ($Sys_Time);}
			}
		}	                                                                               
}