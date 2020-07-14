package org.biodatageeks.sequila.pileup.broadcast

import org.biodatageeks.sequila.pileup.broadcast.Correction.PartitionCorrections
import org.biodatageeks.sequila.pileup.broadcast.Shrink.PartitionShrinks

case class FullCorrections (corrections: PartitionCorrections, shrinks:PartitionShrinks)