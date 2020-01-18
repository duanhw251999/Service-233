#!/usr/bin/perl
use strict;
use FileHandle;
use Net::FTP;
use Cwd;
use Date::Calc qw(Date_to_Time Time_to_Date Add_Delta_Days);
use File::Copy;
use Encode;

=pod
use Encode;
encode("gbk", decode("utf-8", $str));
encode("utf8", decode("gbk", $str));
=cut

main();	


#=====================================================================
sub main()	#定义main函数
#=====================================================================
{  
  
   # 1.修改所有子目录的名称
   modify_sub_name();
   # 2.进入这些子目录找到txt文件，将其修改为接口名称
   modify_txt_dat();
   # 3.将这些文件进行去掉前两行
    cut_dat();
   # 4.生成dir文件
     buildDir();
}

sub modify_sub_name
{
   my $rootPath="E:/PERSONAL/Duanhw/zhk/";
   chdir($rootPath);
   opendir(DIRFILE,$rootPath);
   my @files=readdir(DIRFILE);
   foreach(@files){
      if($_!~/[.]/){
	     if(length($_)==42){
			#print getcwd."/".$_."\n";
			my $olddir=getcwd."/".$_;
			my $newdir=getcwd."/".substr($_,28,8);
			move $olddir,$newdir;
			#print substr($_,28,8)."\n";
		 }
	  }
   }
   close DIRFILE;
}
=pod
ctei--13086
AidSale--13087
register--13088
bind--13089
gatewayInfo--13090
devNetData--13091
=cut
sub modify_txt_dat{
   my $rootPath="E:/PERSONAL/Duanhw/zhk/";
   chdir($rootPath);
   opendir(DIRFILE,$rootPath);
   my @files=readdir(DIRFILE);
   foreach(@files){
      if($_!~/[.]/){
	     if(length($_)==8){
		    my $zhangqi=$_;
			my $subdir=getcwd."/".$zhangqi."/*";
			my @subfiles=glob( $subdir );
			foreach my $sf (@subfiles){
			   my $oldfile=$sf;
			   my $newfile=getcwd."/".$zhangqi."/";
			   if(getFileName($oldfile)=~/ctei/){
				 $newfile=$newfile."s_${zhangqi}_13086_01.dat";
			   }elsif(getFileName($oldfile)=~/AidSale/){
				 $newfile=$newfile."s_${zhangqi}_13087_01.dat";
			   }elsif(getFileName($oldfile)=~/register/){
				 $newfile=$newfile."s_${zhangqi}_13088_01.dat";
			   }elsif(getFileName($oldfile)=~/bind/){
				 $newfile=$newfile."s_${zhangqi}_13089_01.dat";
			   }elsif(getFileName($oldfile)=~/gatewayInfo/){
				 $newfile=$newfile."s_${zhangqi}_13090_01.dat";
			   }elsif(getFileName($oldfile)=~/devNetData/){
				 $newfile=$newfile."s_${zhangqi}_13091_01.dat";
			   }elsif(getFileName($oldfile)=~/cloudBag/){
				 $newfile=$newfile."s_${zhangqi}_13092_01.dat";
				 }elsif(getFileName($oldfile)=~/recommendRecord/){
				 $newfile=$newfile."s_${zhangqi}_13095_01.dat";
			   }else{
			     $newfile="";
			   }
			   move $oldfile,$newfile;
			  #print $oldfile."----".getFileSize($oldfile)."\n";
			}
		 }
	  }
   }
   close DIRFILE;
}

sub cut_dat{
   my $rootPath="E:/PERSONAL/Duanhw/zhk/";
   chdir($rootPath);
   opendir(DIRFILE,$rootPath);
   my @files=readdir(DIRFILE);
   foreach(@files){
      if($_!~/[.]/){
	     if(length($_)==8){
		    my $zhangqi=$_;
			my $subdir=getcwd."/".$zhangqi."/*";
			my @subfiles=glob( $subdir );
			foreach my $sf (@subfiles){
			    cut_tworow(getcwd."/".$zhangqi."/",getFileName($sf));
			}
		 }
	  }
   }
   close DIRFILE;
}

sub cut_tworow{
  my ($path,$oldname)=@_;
  my $count=0;
  my $newname=$path."temp.txt";
  open FILE,"<$path"."$oldname";
  open NEWFILE ,">$newname";
  while(<FILE>){
     if($count>1){
		print NEWFILE $_;
	 }
     $count=$count+1;
  }
  close FILE;
  close NEWFILE;
  
  unlink $path.$oldname;
  move $newname,$path.$oldname;
}

sub getFileName{
	my ($path)=@_;
	my @Name = split(/\//,$path); #将path按“\”分割，得到一个数组，@表示数组
	my $num = 1; #因为perl数组是从0开始，一会需要将数组的长度减1
	my $count = @Name;  #获取数组的长度
	my $ind = $count-$num; #perl两数相减
	my $fileName = $Name[$ind]; #最终获取文件名
	return  $fileName #打印结果
}

sub getFileSize{
	my($filename)=@_;
	my @args = stat ($filename);
	my $size = $args[7];
	return $size;
}

sub buildDir{
   my $rootPath="E:/PERSONAL/Duanhw/zhk/";
   chdir($rootPath);
   opendir(DIRFILE,$rootPath);
   my @files=readdir(DIRFILE);
   foreach(@files){
      if($_!~/[.]/){
	     if(length($_)==8){
		    my $zhangqi=$_;
			my $subdir=getcwd."/".$zhangqi."/*";
			my @subfiles=glob( $subdir );
			foreach my $sf (@subfiles){
			    my $datName=getFileName($sf);
				my $datSize=getFileSize($sf);
				my @arrStr=split /_/,$datName;
				my $filepath=getcwd."/".$zhangqi."/"."dir.bos_$arrStr[2]_$arrStr[0]$arrStr[1]";
				my $content=$datName." ".$datSize;
				create_new($filepath,$content);
				#s_20190828_13086_01.dat
				#dir.bos_13086_s20191007
			}
		 }
	  }
   }
   close DIRFILE;
}

sub create_new{
  my($path,$content)=@_;
  open NEW0,">$path";
  print NEW0 $content;
  close NEW0;
}
=pod
#=====================================================================
sub readsub  # s_20190913_12046_00.verf
#=====================================================================
{
	my $path0="E:/PERSONAL/Duanhw/zhk/";
	chdir($path0);
	opendir(DIRFILE,$path0);
	my @Files =sort(readdir(DIRFILE));
	foreach (@Files){
		if(-d $_){
			my $zhangqi=$_;
			intosub($path0.$_."/",$_);
			#intosub2($path0.$_."/",$_);
		}
	}
	close DIRFILE;
}

sub intosub
{
  my($path,$zhangqi)=@_;
  print $path."\n";
  
}

sub intosub2
{
  my($path,$zhangqi)=@_;
  opendir(DIRFILE,$path);
  my @Files =sort(readdir(DIRFILE));
  foreach (@Files){
			if($_=~/.dat/){
				 my @namestr=split(/\_/,$_);
				 my $jk=$namestr[2];
				 my @args=stat($path.$_);
         open(VERF,">${path}dir.bos_${jk}_s${zhangqi}");
         print VERF $_." $args[7]\n";
         close VERF;
			}
	}
	close DIRFILE;
}

sub delete2{
	my($old,$newf)=@_;
	 open(FILE1, "<${old}");
	 open(FILE2,">>${newf}");
	 my $count=0;
	 while(<FILE1>){
	 	   $count=$count+1;
	 	   if($count>2){
	 	   	 print FILE2 $_;
	 	   }
	 }
	 close FILE1;
	 close FILE2;
	 unlink "${old}";
	 move("${newf}","${old}")||warn "could not copy files :$!" ;
}

#=====================================================================
sub build_verf  # s_20190913_12046_00.verf  dir.bos_12078_i20190922
#=====================================================================
{
	my $path0="E:/PERSONAL/Duanhw/zhk/";
	opendir(DIRFILE,$path0);
	my $cur=Get_before_time(0);
	my $before=Get_before_time(1);
	#print $cur."\n";
	my @Files =sort(grep(/txt/,readdir(DIRFILE)));
	
	foreach (@Files){
	  if($_=~/$cur/){
		  my @strs=split(/\_/,$_);
		  my @args=stat("E:/SMSVOLTE/".$_);
		  my $verfName0="s_".$before."_12047_00.verf";
		  my $verfName1="dir.bos_12047_s".$before;
		  my @rowstr=split(/\./,$strs[2]);
		  touch_file_verf($verfName0,$verfName1,$args[7],$rowstr[0]);
	  }
	}
	close DIRFILE;
}
#=====================================================================
sub touch_file_verf
#=====================================================================
{
	
    my ($filename,$dirName,$size,$row)=@_;
	open NEWFILE ,">E:/SMSVOLTE/${dirName}";
	my @strs=split(/\./,$filename);
	print NEWFILE  $strs[0]."_001.dat";
	for (my $i=0;$i<13;$i=$i+1){
		print NEWFILE  " ";
	}
	print NEWFILE  $size;
	for (my $i=0;$i<13;$i=$i+1){
		print NEWFILE  " ";
	}
	print NEWFILE  $row;
	for (my $i=0;$i<13;$i=$i+1){
		print NEWFILE  " ";
	}
	my $d1=Get_before_time(1);       #赋值 数据文件日期   #集团数据日期-1天
    my $d2=Get_Time(4);     #赋值 数据文件生成时间
	print NEWFILE  $d1.$d2;
	close NEWFILE;
}


#=====================================================================
sub build_dat # s_20190913_12047_00_001.dat  SMSVOLTE_20190913_3935.txt
#=====================================================================
{
	my $before=Get_before_time(1);
    my $path0="E:/SMSVOLTE/";
	opendir(DIRFILE,$path0);
	my $cur=Get_before_time(0);
	my @Files =sort(grep(/txt/,readdir(DIRFILE)));
	foreach (@Files){
		if($_=~/$cur/){
			my @strs=split(/\_/,$_);
			my $datName="s_".$before."_12047_00_001.dat";
			system("cp $path0".$_." $path0".$datName);
		}

	}
	close DIRFILE;
}


#==================================================================================================
sub ClearFIles #该函数实现::根据传递进来的路径，遍历路径下早于4天的文件，将其全部删除
#==================================================================================================
{
    my ($path)=@_;
	opendir(DIRFILE,$path);
	my $cha=Get_before_time(4);
	my @Files =sort(grep(/$cha/,readdir(DIRFILE)));
	if(@Files.length>0){
		my $del_count=0;
		foreach (@Files){
		print $_." 将被删除\n";
		unlink($path.$_);
		$del_count++;
		}
		print "早于4天前的".$del_count."个数据已经全部被清理";
	}
	close DIRFILE;
}


#==================================================================================================
sub Get_before_time  #该函数实现::根据传递进来的参数,用当前时间减去相应天数，得到对应日期    
#====================================================================================================
{
    my ($cha)=@_;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time()-$cha*24*60*60);
	$year += 1900;
	$mon+=1;
	if ($mon < 10)  { $mon="0".$mon;  } else { $mon=$mon;}
	if ($mday < 10) { $mday="0".$mday; } else { $mday=$mday; }
	my $SysDate=sprintf("%4d%02d%02d",$year,$mon,$mday);
	
	return $SysDate;
}


#=================================================
sub Ftp_put97  #上传文件
#=================================================
{
		my $Back_Path=$_[0];
		my $currentDate=Get_Time(3)-1;
		my @conf=("135.149.64.97","PUTDATA_TISS","Put(1715)Tiss","21","");
		my ($host,$user,$napo,$port,$remote)=@conf;	
		
		my $ftp = Net::FTP->new ($host,Timeout => 30,Passive=>1) or die "Could not connect.\n";  
		
		#登录到FTP服务器  
		$ftp->login($user,$napo) or die "Could not login.\n";  
		
		#切换目录  
		$ftp->cwd($remote);  
		print "[".Get_Time(1)."]"." 即将上传文件.\n";
		opendir(DIRFILE,$Back_Path);
		my @Files = sort(grep(/.verf$|\.dat$/,readdir(DIRFILE)));
		foreach my $f(@Files){
			if($f=~m/$currentDate/){
				$ftp->put($Back_Path."/".$f) or die "Could not put remotefile\n"; 
				print "[".Get_Time(1)."]"." $Back_Path/$f 文件已经上传到97服务器.\n";				
			}
		}
		$ftp->quit; 
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

#=================================================
sub Get_LastTime	#获取前一天日期,日期格式必须为'YYYYMMDD'
#=================================================
{
	 my $Handle_time =@_;       
	 my $y=substr($Sys_Date,0,4);
   my $m=substr($Sys_Date,4,2);
   my $d=substr($Sys_Date,6,2);
   my ($y1, $m1, $d1) = Add_Delta_Days($y + 0, $m + 0, $d + 0,-1);
   my $last_time = sprintf("%4d%02d%02d",$y1, $m1, $d1);
   return ($last_time);                                                
}








#=================================================
sub FTP_Upload()	#数据上传,并做数据备份
#=================================================
{
	print "\n\n===============================================================================\n";
	my $theTime=Get_Time(0);
	print "$theTime == 5 ==  Start FTP_Upload 将数据上传到ETL服务器\n";
	my $Ftp_Conf=Get_Ftp(1);
	my ($Ftp_Dir,$Ftp_Host,$Ftp_User,$Ftp_Pass,$Ftp_Prot)=split(/\|/,$Ftp_Conf);
	my $path0="E:/SMSVOLTE/";
	my $ftp=Net::FTP->new($Ftp_Host);
	my $cur=Get_before_time(1);
	if(!$ftp)
	{
		print("	Can't Connect Ftp:$Ftp_Host");
	}
	else
	{
		$ftp->login($Ftp_User,$Ftp_Pass) or die print("	User Or Password is Error:$ftp->message;");
		print("	Success Connected to ftp:$Ftp_Host\n");
		$ftp->cdup();
		my $remote_direction="/";
		$ftp->cwd($remote_direction) or die print("	Can't change dir $remote_direction：$ftp->message");
		opendir(FILE,$path0) or die print("	Can't open $Change_Path:$_");
		chdir($path0); 
		foreach(readdir(FILE))
		{ 
			if($_=~/dat$/gi || $_=~/dir.bos/)
			{   
				if($_=~/$cur/){
					print "	==	Start update $_\n"; 
				    $ftp->put("$_") or die print("	Can't Upload $_:$ftp->message");
				}
			}
		}

		$ftp->quit;
	}
	print "$theTime == 5 ==  End FTP_Upload\n";
}

#=================================================
sub Get_Ftp	#该函数实现::获取FTP配置内容 
#=================================================
{
	my ($FtpType) = @_; #FtpType : 1.U为从210上传到229;2.D为从结算195下载数据到210
	my $FtpFileName;
	if($FtpType == 1)
		{$FtpFileName = "UpFtp.conf";}
	else
		{
			if($FtpType == 2)	
				{$FtpFileName = "DownFtp.conf";}
			else
				{print"获取FTP_FILE方式有误,\$FtpType::$FtpType,因该是'D'or'U'";}
		}
	chdir("$Script_Path");
  open(FTPUPLOAD,$FtpFileName);
  my @Conf_Files=<FTPUPLOAD>;
  my $Ftp_Conf;
  foreach (@Conf_Files)
  {
    	for (my $i=0;$i<@Conf_Files;$i++)
  	  {
  	  	 chomp($Conf_Files[$i]);
  	  	 my ($Conf_Files_Part1,$Conf_Files_Part2)=split(/=/,$Conf_Files[$i]);
        $Ftp_Conf=$Ftp_Conf.$Conf_Files_Part2."|";
  	  }
  }	
  return $Ftp_Conf;
}

####################################################################################################
#该函数实现::读取文件路径功能                                                                      #
sub Get_File_Path
{
	print "\n\n===============================================================================\n";	
	my ($theTime)=Get_Time(1);
	print "$theTime == 1 == 	Start Get_File_Path :: 读取文件路径\n";
	open(PATH,'Path.conf');
	my $Path_Str;	
	my @Paths=<PATH>;
	foreach my $Load(@Paths)
	{
		$Load=~s/\s//g;
		if($Load=~/\w+/gi)		
		{
		chomp($Load);
	    $Path_Str="$Path_Str"."$Load"."|";
	  }
	}
	print "$theTime == 1 == 	End Get_File_Path\n";
	return $Path_Str;
}


 if ($_=~/bind/){
				 	  delete2($path."/".$_,$path."/"."temp");
				 	  move($path."/".$_,$path."s_${zhangqi}_13087_01.dat");
				 }
				 if ($_=~/ctei/){
				 	  delete2($path."/".$_,$path."/"."temp");
				 	  move($path."/".$_,$path."s_${zhangqi}_13087_01.dat");
				 }
				 if ($_=~/register/){
				 	  delete2($path."/".$_,$path."/"."temp");
				 	  move($path."/".$_,$path."s_${zhangqi}_13087_01.dat");
				 }
				 if ($_=~/gatewayInfo/){
					 	  delete2($path."/".$_,$path."/"."temp");
				 	  move($path."/".$_,$path."s_${zhangqi}_13087_01.dat");
				 }
				 if ($_=~/devNetData/){
				 	  delete2($path."/".$_,$path."/"."temp");
				 	  move($path."/".$_,$path."s_${zhangqi}_13087_01.dat");
				 }
				 
				 
				 chdir($path);
  opendir(DIRFILE,$path);
  my @Files =readdir(DIRFILE);
  foreach (@Files){
  	
			if($_=~/.txt/){
				print $_."\n";
				#delete2($path."/".$_,$path."/"."temp");
				print $path.$_."\n";
			}
	}
	close DIRFILE;
=cut
