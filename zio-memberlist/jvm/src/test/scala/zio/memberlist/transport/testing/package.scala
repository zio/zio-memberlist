package zio.memberlist.transport

import zio.Has

package object testing {

  type InMemoryTransport = Has[InMemoryTransport.Service]

}
