<?php
/**
 * Credis, a Redis interface for the modest
 *
 * @author Justin Poliey <jdp34@njit.edu>
 * @copyright 2009 Justin Poliey <jdp34@njit.edu>
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 * @package Credis
 */

#require_once 'Credis/Client.php';

/**
 * A generalized Credis_Client interface for a cluster of Redis servers
 */
class Credis_Cluster
{

  /**
   * Collection of Credis_Client objects attached to Redis servers
   * @var Credis_Client[]
   */
  protected $clients;
  
  /**
   * Aliases of Credis_Client objects attached to Redis servers, used to route commands to specific servers
   * @see Credis_Cluster::to
   * @var array
   */
  protected $aliases;
  
  /**
   * Hash ring of Redis server nodes
   * @var array
   */
  protected $ring;
  
  /**
   * Individual nodes of pointers to Redis servers on the hash ring
   * @var array
   */
  protected $nodes;
  
  /**
   * The commands that are not subject to hashing
   * @var array
   * @access protected
   */
  protected $dont_hash;

  /**
   * Creates an interface to a cluster of Redis servers
   * Each server should be in the format:
   *  array(
   *   'host' => hostname,
   *   'port' => port,
   *   'timeout' => timeout,
   *   'alias' => alias
   * )
   *
   * @param array $servers The Redis servers in the cluster.
   * @param int $replicas
   */
  public function __construct($servers, $replicas = 128)
  {
    $this->clients = array();
    $this->aliases = array();
    $this->ring = array();
    $clientNum = 0;
    foreach ($servers as $server)
    {
      $client = new Credis_Client($server['host'], $server['port'], isset($server['timeout']) ? $server['timeout'] : 2.5, isset($server['persistent']) ? $server['persistent'] : '');
      $this->clients[] = $client;
      if (isset($server['alias'])) {
        $this->aliases[$server['alias']] = $client;
      }
      for ($replica = 0; $replica <= $replicas; $replica++) {
        $this->ring[crc32($server['host'].':'.$server['port'].'-'.$replica)] = $clientNum;
      }
      $clientNum++;
    }
    ksort($this->ring, SORT_NUMERIC);
    $this->nodes = array_keys($this->ring);
    $this->dont_hash = array_flip(array(
      'RANDOMKEY', 'DBSIZE', 'PIPELINE', 'EXEC',
      'SELECT',    'MOVE',    'FLUSHDB',  'FLUSHALL',
      'SAVE',      'BGSAVE',  'LASTSAVE', 'SHUTDOWN',
      'INFO',      'MONITOR', 'SLAVEOF'
    ));
  }

  /**
   * Get a client by index or alias.
   *
   * @param string|int $alias
   * @throws CredisException
   * @return Credis_Client
   */
  public function client($alias)
  {
    if (is_int($alias) && isset($this->clients[$alias])) {
      return $this->clients[$alias];
    }
    else if (isset($this->aliases[$alias])) {
      return $this->aliases[$alias];
    }
    throw new CredisException("Client $alias does not exist.");
  }

  /**
   * Get an array of all clients
   *
   * @return array|Credis_Client[]
   */
  public function clients()
  {
    return $this->clients;
  }

  /**
   * Execute a command on all clients
   *
   * @return array
   */
  public function all()
  {
    $args = func_get_args();
    $name = array_shift($args);
    $results = array();
    foreach($this->clients as $client) {
      $results[] = $client->__call($name, $args);
    }
    return $results;
  }

  /**
   * Get the client that the key would hash to.
   *
   * @param string $key
   * @return \Credis_Client
   */
  public function byHash($key)
  {
    return $this->clients[$this->hash($key)];
  }

  /**
   * Execute a Redis command on the cluster with automatic consistent hashing
   *
   * @param string $name
   * @param array $args
   * @return mixed
   */
  public function __call($name, $args)
  {
    if (isset($this->dont_hash[strtoupper($name)])) {
      $client = $this->clients[0];
    }
    else {
      $client = $this->byHash($args[0]);
    }

    return $client->__call($name, $args);
  }

  /**
   * Get client index for a key by searching ring with binary search
   *
   * @param string $key The key to hash
   * @return int The index of the client object associated with the hash of the key
   */
  public function hash($key)
  {
    $needle = crc32($key);
    $server = $min = 0;
    $max = count($this->nodes) - 1;
    while ($max >= $min) {
      $position = (int) (($min + $max) / 2);
      $server = $this->nodes[$position];
      if ($needle < $server) {
        $max = $position - 1;
      }
      else if ($needle > $server) {
        $min = $position + 1;
      }
      else {
        break;
      }
    }
    return $this->ring[$server];
  }

}

