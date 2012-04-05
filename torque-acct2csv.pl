#!/usr/bin/perl -w
use strict;
use Text::CSV;
use DateTime::Format::DateParse;

my %jobs;

my %states = (
	'Q' => 'queued',
	'S' => 'started',
	'E' => 'finished',
	'D' => 'deleted'
);

sub to_bytes($) {
	return undef unless $_[0];
	my ($n,$u)=$_[0]=~/^(\d+)(\w+)$/;

	if ($u =~ /^[bw]$/i) {
		return($n);
	} elsif ($u =~ /^k[bw]$/i) {
		return($n*1024);
	} elsif ($u =~ /^m[bw]$/i) {
		return($n*1048576);
	} elsif ($u =~ /^g[bw]$/i) {
		return($n*1073741824);
	} elsif ($u =~ /^t[bw]$/i) {
		return($n*1099511627776);
	}

	die("to_bytes: Wrong value $_[0]");
}

sub to_secs($) {
	return undef unless $_[0];
	my ($h,$m,$s)=split(/:/,$_[0],3);
	return($h*3600+$m*60+$s);
}

### Parse accounting logs #############
# http://www.clusterresources.com/torquedocs/9.1accounting.shtml

while(<>) {
	chomp;

	# 0=timestamp,1=state,2=job_id3=data
	my ($ts,$state,$id,$data)=split(/;/,$_,4);
	my $dt=DateTime::Format::DateParse->parse_datetime($ts);

	unless (exists $jobs{$id}) {
		$jobs{$id} = {data => {}};
	}

	# update last job state
	$jobs{$id}->{state} = $state;
	$jobs{$id}->{lc($state.'_time')} = $dt;
	$jobs{$id}->{data} = {
		map { my @d=split(/=/,$_,2); $d[0] => $d[1] } split(/ /,$data)
	};
}


### Dump CSV ##########################

my $csv = Text::CSV->new({eol => $/})
	or die "Cannot use CSV: ".Text::CSV->error_diag();

# print header
$csv->print(\*STDOUT,[
	'Job ID',
	'Owner',
	'State',
	'Exit status',
	'Queue',
	'Created at',
	'Time start',
	'Time exit',
	'Time delete',
	'Node',
	'Req. nodes',
	'Req. procs',
	'Req. mem',
	'Req. vmem',
	'Req. walltime',
	'Used mem',
	'Used vmem',
	'Used walltime',
	'Used cputime',
]);

for my $id (sort { ($a=~/^(\d+)/)[0] <=> ($b=~/^(\d+)/)[0] } keys %jobs) {
	my $j=$jobs{$id};
	my $d=$j->{data};
	my $q_time = $j->{q_time};

	my $cols = [
		$id, #(split(/\./,$id))[0],					# 0: Job ID
		$d->{user},									# 1: Owner
		$states{$j->{state}},						# 2: State
		$d->{Exit_status},							# 3: Exit status
		$d->{queue},								# 4: Queue
		$q_time->ymd,								# 5: Created at

		# *** job times *******************************************

		# 6: Time start
		$j->{s_time} ?
			$j->{s_time}->epoch - $q_time->epoch :
			undef,

		# 7: Time exit
		$j->{e_time} ?
			$j->{e_time}->epoch - $q_time->epoch :
			undef,

		# 8: Time delete
		$j->{d_time} ?
			$j->{d_time}->epoch - $q_time->epoch :
			undef,

		# 9: Nodes, take only first 
		$d->{exec_host} ?
			(split(/\//,$d->{exec_host}))[0] :
			undef,

		# *** requested resources *********************************

		# 10: Req. nodes
		$d->{'Resource_List.processed_nodes'} ?
			($d->{'Resource_List.processed_nodes'} =~ /^(\d+):/)[0] :
			undef,

		# 11: Req. procs
		$d->{'Resource_List.processed_nodes'} ?
			($d->{'Resource_List.processed_nodes'} =~ /ppn=(\d+)/)[0] :
			undef,

		# 12, 13, 14: Req. mem, vmem, walltime
		to_bytes($d->{'Resource_List.mem'}),
		to_bytes($d->{'Resource_List.vmem'}),
		to_secs($d->{'Resource_List.walltime'}),

		# *** used resources **************************************

		# 15, 16, 17, 18: Used mem, vmem, walltime, cputime
		to_bytes($d->{'resources_used.mem'}),
		to_bytes($d->{'resources_used.vmem'}),
		to_secs($d->{'resources_used.walltime'}),
		to_secs($d->{'resources_used.cput'})
	];

	# fix CPU time
	if (defined $cols->[18]) {
		# req_nodes * req_ppn * used_walltime
		my $max_cputime = $cols->[10] * $cols->[11] * $cols->[17];
		if ($cols->[18] > $max_cputime) {
			warn("$cols->[0]: Fixing cputime $cols->[18] to $max_cputime");
			$cols->[18] = $max_cputime;
		}
	}

	$csv->print(\*STDOUT,$cols);
}
