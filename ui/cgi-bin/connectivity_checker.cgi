#!/usr/bin/perl
use strict;
use warnings;
use CGI;
my $cgi = CGI->new;
print $cgi->header('application/json');
use POSIX qw(strftime);
# Remote SSH credentials (use environment variables or configure your credentials)
my $remote_user = $ENV{REMOTE_USER} || 'your_ssh_user';
my $remote_pass = $ENV{REMOTE_PASS} || 'your_ssh_password';

my $action       = $cgi->param('action')    || '';
my $remote_host  = $cgi->param('server')    || '';
my $interface    = $cgi->param('interface') || '';
my $source_ip    = $cgi->param('source_ip');
my $dst_ip    = $cgi->param('dst_ip');
my $port         = $cgi->param('port')         || '';
my $packet_count = $cgi->param('packet_count') || '';
my $duration     = $cgi->param('duration')     || 60;
my $protocol     = $cgi->param('protocol')     || '';
my $and_grep_filter     = $cgi->param('and_grep')     || '';
my $or_grep_filter     = $cgi->param('or_grep')     || '';
my $generate_pcap     = $cgi->param('generate_pcap')     || "";
# Build tcpdump command
my $command = "tcpdump -vvv -nn ";

$command .= " -i $interface"    if $interface && lc($interface) ne "any";
$command .= " -c $packet_count" if $packet_count;

my $filters = "";
$filters .= " port $port" if $port;
if ($source_ip) {

    $filters .= " and src $source_ip" if $filters;
    $filters .= " src $source_ip" unless $filters;
}
if ($dst_ip) {

    $filters .= " and dst $source_ip" if $filters;
    $filters .= " dst $source_ip" unless $filters;
}

if ( $protocol && lc($protocol) ne "any" ) {

    $filters .= " and  $protocol" if $filters;
    $filters .= " $protocol" unless $filters;
}

if ($or_grep_filter) {
    chomp($or_grep_filter); 
  my @lines = split /\n/, $or_grep_filter;

    # Trim and discard fully blank lines
    @lines = grep { $_ ne '' } map { s/^\s+|\s+$//gr } @lines;

    if (@lines) {
        $or_grep_filter = join("' -e '", @lines);
        $or_grep_filter = "| grep -e '" . $or_grep_filter . "'";
    } else {
        $or_grep_filter = '';  # Avoid generating | grep -e ''
    }
}

if ($and_grep_filter) {
    chomp($and_grep_filter); 
if ($and_grep_filter !~ /^[a-zA-Z0-9 .]+$/) {
    die "Invalid input!";
}
    $and_grep_filter =" | grep -i '".$and_grep_filter."'";
}
$and_grep_filter=$or_grep_filter.$and_grep_filter;
# Append filters to the command if any filters are present
$command .= $filters;


my $cgi_id   = $$;
my $log_dir  = "logs";
my $log_file = "$log_dir/tcpdump_output_$cgi_id.log";
my $pid      = "";
my $pid_file = "";
my $pcap_file_name="";
my $pcap_probe_file_location="/tmp";
if ($generate_pcap && $generate_pcap eq "yes") {
    chomp($generate_pcap);


    my $timestamp = strftime("%Y%m%d_%H%M%S", localtime);
    $pcap_file_name = "${cgi_id}.pcap";
   # Build the tcpdump option
    $generate_pcap = " -w $pcap_probe_file_location/$pcap_file_name";
    $command="$command $generate_pcap";
}


# Remove log files from /tmp older than 1 day
system("find /tmp -name 'tcpdump_pid_*.txt' -mtime +1 -type f -delete 2>/dev/null &");
system("find '$log_dir' -name '*.log' -mtime +1 -type f -delete 2>/dev/null &");

my $final_command = "echo $remote_pass | sudo -S /sbin/$command";

if ( $action eq 'validate' ) {

    my $validate_cmd = qq(timeout 15 sshpass -p '$remote_pass' ssh -tt -q -o StrictHostKeyChecking=no $remote_user\@$remote_host "$final_command -d");
    my @dry_run      = qx{$validate_cmd 2>&1};
    @dry_run = ('Command failed: no output received') unless @dry_run;
    $command=$command.$and_grep_filter;

    my $exit_code = $? >> 8;
    if ( $exit_code != 0 ) {

        print_json_response( { status => "failed", pid => $cgi_id, pcap => $pcap_file_name, command => $command, message => join( "\n", clean_output(@dry_run) ) } );

    }
    else {
        print_json_response( { status => "valid", pid => $cgi_id, pcap => $pcap_file_name, command => $command, message => join( "\n", clean_output(@dry_run) ) } );
    }
}
elsif ( $action eq 'start' ) {

    ###################### not working foe me
    #my $ssh_cmd=qq(timeout $duration sshpass -p '$remote_pass' ssh -tt -q -o StrictHostKeyChecking=no $remote_user\@$remote_host "$command");
    $final_command=$final_command.$and_grep_filter;
    $command=$command.$and_grep_filter;
    my $ssh_cmd = qq(timeout $duration sshpass -p '$remote_pass' ssh -tt -q -o StrictHostKeyChecking=no $remote_user\@$remote_host "$final_command");


    #   system("nohup sh -c '$ssh_cmd' > $log_file 2>&1 &");
    my $cmd = "nohup sh -c '$ssh_cmd' > $log_file 2>&1 & echo \$!";

    #  my $cmd = "nohup sh -c '\'$ssh_cmd\'' > $log_file 2>&1 & echo \$!";

    $pid = `$cmd`;

    chomp($pid);
    $pid_file = "/tmp/tcpdump_pid_$pid.txt";

    # Save the PID to a file
    write_file( $pid_file, $pid );

    print_json_response( { status => "started", id => $pid, pid => $cgi_id, command => $command,  pcap => $pcap_file_name } );

    #}
}
elsif ( $action eq 'status' ) {
    my $pid = $cgi->param('id') || '';
    $cgi_id = $cgi->param('pid') || $pid;
    $pcap_file_name=$cgi->param('pcap') || '';
    if ( !$pid ) {
        print_json_response( { error => "Missing id parameter" } );
        exit 20;
    }
    $pid_file = "/tmp/tcpdump_pid_$pid.txt";

    if ( -e $pid_file ) {
        my $pid = read_file($pid_file);
        chomp($pid);

        # Check if the process is still running
        my $status = qx{ps -p $pid -o comm=};
        if ($status) {
            print_json_response( { status => "running", output => get_output($cgi_id), id => $pid, pid => $cgi_id , pcap => $pcap_file_name } );
        }
        else {
            scp_pcap_to_server($pcap_probe_file_location, $pcap_file_name,$log_dir);
            print_json_response(
                {   status => "completed",

                    id      => $pid,
                    output  => get_output($cgi_id),
                    pid     => $cgi_id,
                    pcap => $pcap_file_name,
                    command => "ps -p $pid -o comm="
                }
            );

        }
    }
    else {
        print_json_response( { status => "invalid", message => "No process found" } );
    }
}
elsif ( $action eq 'stop' ) {

    # Validate that the 'id' parameter is passed
    my $pid = $cgi->param('id') || '';
    $cgi_id = $cgi->param('pid') || '';
    $pcap_file_name=$cgi->param('pcap') || '';

    if ( !$pid ) {
        print_json_response( { error => "Missing id parameter" } );
        exit 20;
    }
    $pid_file = "/tmp/tcpdump_pid_$pid.txt";

    if ( -e $pid_file ) {
        my $pid = read_file($pid_file);
        chomp($pid);

        # Kill the process
        qx{kill $pid};

        # Clean up PID file
        unlink $pid_file;
        scp_pcap_to_server($pcap_probe_file_location, $pcap_file_name,$log_dir);
        print_json_response( { status => "stopped", id => $pid, pid => $cgi_id, output => get_output($cgi_id), pcap => $pcap_file_name  } );
    }
    else {
        print_json_response( { status => "no process found" } );
    }
}

# Default case: Invalid action
else {
    my $o = "";
    foreach my $param ( $cgi->param ) {
        $o = $o . " $param: " . $cgi->param($param);
    }
    print_json_response( { error => "Invalid action", status => $o } );
}

sub get_output {

    my ($file_id) = @_;

    $log_file = "$log_dir/tcpdump_output_$file_id.log";
    if ( -e $log_file ) {
        my @output = qx{head -100 $log_file};
        @output = clean_output(@output);
        return join( "\n", @output );
    }
    else { return "output file $file_id not found"; }

}

sub clean_output {
    return grep { $_ !~ /^(Pseudo-terminal|Last login|sudo|\[sudo\]|banner|Connection to .* closed|\s*dropped privs to tcpdump)/i } @_;
}

sub print_json_response {
    my ($response) = @_;
    print "{\n";
    my $last_key = ( keys %{$response} )[-1];
    foreach my $key ( keys %{$response} ) {
        my $comma = ( $key eq $last_key ) ? "" : ",";
        print '  "' . $key . '": ' . json_string( $response->{$key} ) . $comma . "\n";
    }
    print "}\n";
}

sub json_string {
    my ($str) = @_;
    $str =~ s/\\/\\\\/g;    # Escape backslashes
    $str =~ s/"/\\"/g;      # Escape double quotes
    $str =~ s/\n/\\n/g;     # Escape newlines
    $str =~ s/\r/\\r/g;     # Escape carriage returns
    $str =~ s/\t/\\t/g;     # Escape tabs
    return '"' . $str . '"';
}

sub write_file {
    my ( $file, $content ) = @_;

    # Open the file in write mode, or create the file if it doesn't exist
    open my $fh, '>', $file or die "Cannot open file $file: $!";

    # Write the content to the file
    print $fh $content;

    # Close the file
    close $fh or die "Cannot close file $file: $!";
}

sub read_file {
    my ($file) = @_;
    open my $fh, '<', $file or return '';
    local $/;
    my $content = <$fh>;
    close $fh;
    return $content;
}

sub scp_pcap_to_server {
    my ($pcap_probe_file_location, $pcap_file, $webgui_path) = @_;
    $pcap_file="$pcap_probe_file_location/$pcap_file";
    chomp($pcap_file);

    my $scp_command = "sshpass -p '$remote_pass' scp -o StrictHostKeyChecking=no $remote_user\@$remote_host:$pcap_file $webgui_path";

    my $status = system($scp_command);

    if ($status == 0) {
      
        return 1;
    } else {
        return 0;
    }
}
